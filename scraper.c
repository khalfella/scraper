#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/avl.h>
#include <string.h>
#include <math.h>
#include <stddef.h>


#define NTHREADS	8
#define	BUFFSIZE	(1024*1024)


#define	MIN(a,b) (((a)<(b))?(a):(b))


typedef struct strnode_s {
	char			*n_str;		/* Pointer to word	*/
	unsigned long long	n_count;	/* Frequency		*/
	avl_node_t		n_avl;		/* AVL node 		*/
} strnode_t;

typedef struct scraper_s {
	int			s_idx;
	pthread_t		s_thread;	/* scraper thread	*/
	off_t			s_start;	/* start scraping offset*/
	off_t			s_end;		/* end scraping offset	*/
	int 			s_fd;		/* file to scrape	*/
	struct scraper_s 	*s_next;	/* merge scraper	*/
	avl_tree_t		*s_tree;	/* avl tree of words	*/
} scraper_t;


/*
 * Helper routines
 */

int is_power_of_two(int x) {
  return x && (!(x&(x-1)));
}

static char *
safe_strdup(char *str) {
	char *ret;

	if ((ret = strdup(str)) == NULL) {
		fprintf(stderr, "failed to allocate memory\n");
		exit(2);
	}
	return ret;
}

static void *
safe_malloc(size_t sz) {
	char *ret;
	if((ret = malloc(sz)) == NULL) {
		fprintf(stderr, "failed to allocate memory\n");
		exit(2);
	}
	return (ret);
}

static int
pread_all(int fd, char *buf, ssize_t count, off_t offset)
{
	int rd = 1, ret = 0;
	while (rd > 0 && ret < count) {
		rd = pread(fd, buf + ret, count - ret, offset + ret);
		if (rd == -1)
			return (-1);
		ret += rd;
	}
	return (ret);
}

/*
 * avl tree nodes comparator
 */

static int
strnode_comparator(const void *l, const void *r) {
	int cmp;
	strnode_t *ln = (strnode_t *) l;
	strnode_t *rn = (strnode_t *) r;

	if ((cmp = strcmp(ln->n_str, rn->n_str)) != 0)
		return cmp/abs(cmp);

	return (0);
}

/*
 * Initialize scraper struct.
 */

static void
sc_init(scraper_t *sc, int idx, int fd, off_t start,
    off_t end, off_t tsize, int nthreads) {
	char c;

	sc->s_idx = idx;
	sc->s_fd = fd;
	sc->s_start = start;
	sc->s_end = end;
	sc->s_next = NULL;
	sc->s_tree = NULL;

	/* adjust the start and end offsets */
	if (sc->s_start != 0) {
		pread(fd, &c, 1, sc->s_start - 1); 
		if (c != '\n') {
			do {
				pread(fd, &c, 1, sc->s_start);
				sc->s_start++;
			} while (c != '\n' && sc->s_start <= sc->s_end);
		}
	}

	if (idx == nthreads - 1) {
		sc->s_end = tsize - 1;
	} else {
		pread(fd, &c, 1, sc->s_end);
		while (c != '\n') {
			sc->s_end++;
			pread(fd, &c, 1, sc->s_end);
		}
	}
}


/*
 * Adds word to scraper avl tree.
 * increment the counter if the
 * word already exists.
 */

static void
sc_add_word(scraper_t *sc,char *word) {

	strnode_t snode;
	strnode_t *np;

	snode.n_str = word;
	if ((np = avl_find(sc->s_tree, &snode, NULL)) != NULL) {
		np->n_count++;
		return;
	}

	np = safe_malloc(sizeof (strnode_t));
	np->n_str = safe_strdup(word);
	np->n_count = 1;
	avl_add(sc->s_tree, np);
}

/*
 * Scraper thread main function:
 *     1- Initialize scraper AVL tree
 *     2- Read the words betwee start and end [start, end)
 *     3- Adds recognized words to AVL tree.
 */

static void *
sc_work(void *arg) {
	scraper_t *sc;
	char *buff, *word, *s;
	int boff, bsize;

	sc = (scraper_t *) arg;
	buff = safe_malloc(BUFFSIZE);
	boff = bsize = 0;
	s = word = malloc(1024);

	sc->s_tree = safe_malloc(sizeof (avl_tree_t));
	avl_create(sc->s_tree,
	    strnode_comparator,
	    sizeof (strnode_t),
	    offsetof(strnode_t, n_avl));


	while (sc->s_start < sc->s_end) {

		if (boff >= bsize) {
			boff = 0;
			bsize = pread_all(sc->s_fd, buff,
			    MIN(BUFFSIZE,  (sc->s_end - sc->s_start +1)),
			    sc->s_start);
			sc->s_start += bsize;
		}
next_word:
		while (boff < bsize && buff[boff] != '\n') {
			*s++ = buff[boff];
			boff++;
		}

		if (boff >= bsize)
			continue;

		*s++ = 0; boff++;

		sc_add_word(sc, word);
		s = word;
		goto next_word;
	}


	free(word);
	free(buff);
	return (NULL);
}

/*
 * Debugging routines
 */
/*
static void
sc_show(scraper_t *sc, int idx) {
	printf("---- Thread %d-----\n", idx);
	printf("sc[%d].s_fd = %d\n", idx, sc->s_fd);
	printf("sc[%d].s_start = %lld\n", idx, sc->s_start);
	printf("sc[%d].s_end = %lld\n", idx, sc->s_end);
	printf("sc[%d].a_size = %lld\n", idx, sc->s_end - sc->s_start + 1);
}

static void
sc_print_tree(scraper_t *sc) {
	strnode_t *np;
	for (np = avl_first(sc->s_tree); np != NULL;
	    np = AVL_NEXT(sc->s_tree, np)) {
		printf("scraper [%d] \"%s\" = [%lld]\n",
		    sc->s_idx, np->n_str, np->n_count);
	}
}
*/

/*
 * Merger thread routine
 */

static void *
sc_merge(void *arg) {
	scraper_t *sc1, *sc2;
	strnode_t *np, *np1, *np2;
	int cmp;

	sc1 = (scraper_t *) arg;
	if ((sc2 = sc1->s_next) == NULL)
		return (NULL);

	np1 = avl_first(sc1->s_tree);
	np2 = avl_first(sc2->s_tree);

	while (np1 != NULL || np2 != NULL) {
		if (np1 != NULL && np2 != NULL) {
			if ((cmp = strcmp(np1->n_str, np2->n_str)) == 0) {
				np1->n_count += np2->n_count;
				np2->n_count = 0; /* might be unnecessary */
				np1 = AVL_NEXT(sc1->s_tree, np1);
				np2 = AVL_NEXT(sc2->s_tree, np2);
			} else if (cmp < 0) {
				np1 = AVL_NEXT(sc1->s_tree, np1);
			} else {
				np = AVL_NEXT(sc2->s_tree, np2);
				avl_remove(sc2->s_tree, np2);
				avl_add(sc1->s_tree, np2);
				np2 = np;
			}
		} else if (np1 == NULL && np2 != NULL) {
			np = AVL_NEXT(sc2->s_tree, np2);
			avl_remove(sc2->s_tree, np2);
			avl_add(sc1->s_tree, np2);
			np2 = np;
		} else {	/* np1 != NULL && np2 == NULL */
			break;
		}
	}
	return (NULL);
}


/*
 * Main work
 *     1- Create scraper threads
 *     2- Wait for scrapers to finish
 *     3- Start merging threads
 *     4- Wait for merging threads to finish
 */

static void
scrape(scraper_t *sc, int nthreads) {

	int i;
	void *status;

	/* Start scrapers threads */
	for (i = 0; i < nthreads; i++) {
		if (pthread_create(&sc[i].s_thread, NULL, sc_work, &sc[i]) != 0) {
			fprintf(stderr, "Failed to create scraper thread\n");
			exit(2);
		}
	}

	for (i = 0; i < nthreads; i++)
		pthread_join(sc[i].s_thread, &status);


	/* Merge scrapers' AVL trees */
	while (nthreads != 1) {
		for (i = 0; i < nthreads / 2; i++) {
			sc[i].s_next = &sc[i + nthreads / 2];
			if (pthread_create(&sc[i].s_thread, NULL, sc_merge, &sc[i]) != 0) {
				fprintf(stderr, "Failed to create merger thread\n");
				exit(2);
			}
		}

		for (i = 0; i < nthreads / 2; i++) {
			pthread_join(sc[i].s_thread, &status);
		}
		nthreads /= 2;
	}
}

/*
 * Print output frequency:word
 */

static void
output(scraper_t *sc) {
	strnode_t *np;
	for (np = avl_first(sc->s_tree); np != NULL;
	    np = AVL_NEXT(sc->s_tree, np)) {
			printf("%lld:%s\n", np->n_count, np->n_str);
	}
}


/*
 * Usage function
 */

static void
usage(int ecode) {
	fprintf(stderr, "scraper -f filename [-t nthreads]\n");
	exit(ecode);
}

/*
 * Main function
 */

int main(int argc, char **argv) {
	char c;
	int fd, i, nthreads;
	char *filename;
	struct stat sbuf;
	scraper_t *sc;

	nthreads = NTHREADS;
	filename = NULL;

	while((c = getopt(argc, argv, "ht:f:")) != EOF) {
		switch (c) {
			case 't':
				if ((nthreads = strtol(
				    optarg, NULL, 10)) <= 0) {
					fprintf(stderr, "Invalid number of "
					    "threads \"%s\"\n", optarg);
					exit(1);
				}
				break;
			case 'f':
				filename = optarg;
				break;
			case 'h':
				usage(0);
			default:
				usage(1);
				break;
		}
	}

	if (!is_power_of_two(nthreads)) {
		fprintf(stderr, "nthreads is not power of two\n");
		exit(2);
	}

	if (filename == NULL) {
		fprintf(stderr, "Missing filename\n");
		usage(1);
	}

	if ((fd = open(filename, O_RDONLY)) == -1) {
		fprintf(stderr, "failed to open %s\n", filename);
		return (1);
	}


	if (fstat(fd, &sbuf) == -1) {
		fprintf(stderr, "failed to stat %s\n", filename);
		return (1);
	}


	sc = safe_malloc(sizeof (scraper_t) * nthreads);

	for (i = 0; i < nthreads; i++) {
		sc_init(&sc[i], i, fd,				/* idx, fd */
		    i * (sbuf.st_size / nthreads),		/* start   */
		    (i + 1) * (sbuf.st_size / nthreads) - 1,	/* end     */
		    sbuf.st_size,				/* tsize   */
		    nthreads);					/* threads */
	}

	/* Start scrapers and print output */
	scrape(sc, nthreads);
	output(sc);
	return (0);
}

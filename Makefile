all:	scraper

scraper:	scraper.c
	gcc -g -ggdb3 -D_FILE_OFFSET_BITS=64 -Wall -Wextra scraper.c -lavl -o scraper

clean:
	rm -rf scraper

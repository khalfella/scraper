# scraper: Multi-threaded word frequency C program

`scraper -f <filename> -t 8` has the same functionality of `cat <filename> | sort | uniq -c` except:

* scraper is faster because it is multithreaded
* scraper consumes less memory

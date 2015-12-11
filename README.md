# scrape-itebook

Developed using Python 3.4

Scrape the site http://it-ebooks.info/ and save all the ebooks.

## Dependencies
- [BeautifulSoup4](https://pypi.python.org/pypi/beautifulsoup4)
- [SQLAlchemy](https://pypi.python.org/pypi/SQLAlchemy)
- [custom_utils](https://github.com/xtream1101/custom-utils)

## Usage
- Any args passed in via the command line will override values in the config file if one is passed in
- You must pass a config file with `save_dir` set or `-d` 

`$ python3 main.py -c <config_file> -d </dir/to/download/dir>`  
Set this to run as a cron to keep up to date with the content


## Config file
All values in the config file are optional  
If you do not have `save_dir` set here, you must pass in the path using `-d`  
```
save_dir: ./it-ebooks
restart: false

proxies: 
    - http://xx.xx.xx.xx:xx
    - http://xx.xx.xx.xx:xx
    - http://xx.xx.xx.xx:xx
```

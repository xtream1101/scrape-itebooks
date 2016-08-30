import re
import sys
import time
import signal
import logging
from models import db_session, Setting, Book, NoResultFound
from scraper_monitor import scraper_monitor
import custom_utils as cutil
from scrapers import Scraper, Web

# Create logger for this script
logger = logging.getLogger(__name__)


class Worker:

    def __init__(self, web, book_id):
        """
        Worker Profile

        Run for each item that needs parsing
        Each thread has a web instance that is used for parsing
        """
        # `web` is what utilizes the profiles and proxying
        self.web = web
        self.book_id = str(book_id)

        # Get the sites content as a beautifulsoup object
        url = '{base_url}/book/{book_id}/'.format(base_url=self.web.scraper.BASE_URL,
                                                  book_id=self.book_id)
        soup = self.web.get_site(url, page_format='html')
        if soup is None:
            logger.warning("Response was None for url {url}".format(url=url))
            return

        if soup.find('img', {'alt': 'Page Not Found'}):
            logger.warning("Page Not Found: {url}".format(url=url))
            return

        logger.info("Getting book {book_id}".format(book_id=self.book_id))

        data = self.parse_book(soup)

        if data is not None:
            # Add raw data to db
            self.web.scraper.insert_data(data)

            # Add success count to stats. Keeps track of how much ref data has been parsed
            self.web.scraper.track_stat('ref_data_success_count', 1)

        # Take it easy on the site
        time.sleep(1)

    def parse_book(self, content):
        """
        :return: List of items with their details
        """
        cover_source = content.find('img', {'itemprop': 'image'})['src'].strip()
        try:
            subtitle = content.find('h3').getText().strip()
        except AttributeError:
            subtitle = None

        parsed_data = {'book_id': self.book_id,
                       'file_location': None,
                       'file_cover_location': None,
                       'file_cover_source': self.web.scraper.BASE_URL + cover_source,
                       'description': content.find('span', {'itemprop': 'description'}).getText().strip(),
                       'file_source': content.find('a', {'href': re.compile('http://filepi.com')})['href'],
                       'format': content.find(attrs={'itemprop': 'bookFormat'}).getText().strip().lower(),
                       'isbn': content.find(attrs={'itemprop': 'isbn'}).getText().strip(),
                       'language': content.find(attrs={'itemprop': 'inLanguage'}).getText().strip(),
                       'pages': content.find(attrs={'itemprop': 'numberOfPages'}).getText().strip(),
                       'publisher': content.find(attrs={'itemprop': 'publisher'}).getText().strip(),
                       'title': content.find('h1', {'itemprop': 'name'}).getText().strip(),
                       'subtitle': subtitle,
                       'year': content.find(attrs={'itemprop': 'datePublished'}).getText().strip(),
                       'author': content.find(attrs={'itemprop': 'author'}).getText().strip(),
                       'time_collected': cutil.get_datetime(),
                       }

        # Download book
        base_filename = '{last_nums}/{book_id}/{book_id}'\
                        .format(last_nums=self.book_id[-2:], book_id=self.book_id)

        book_filename = '{base_filename}_book.{ext}'.format(base_filename=base_filename,
                                                            ext=parsed_data.get('format'))
        cover_ext = cutil.get_file_ext(parsed_data.get('file_cover_source'))
        book_cover_filename = '{base_filename}_cover{ext}'.format(base_filename=base_filename,
                                                                  ext=cover_ext)
        parsed_data['file_cover_location'] = self.web.download(parsed_data.get('file_cover_source'),
                                                               book_cover_filename)

        header = {'Referer': self.web.scraper.BASE_URL}
        parsed_data['file_location'] = self.web.download(parsed_data.get('file_source'),
                                                         book_filename,
                                                         header=header)

        return parsed_data


class ItEbooks(Scraper):

    def __init__(self, config_file=None):
        super().__init__('itebooks')

        self.BASE_URL = 'https://it-ebooks.info'
        self.book_ids = self.get_latest_books()
        self.last_id_scraped = self.get_last_scraped()

    def start(self):
        """
        Send the ref data to the worker threads
        """
        if len(self.book_ids) == 0:
            logger.critical("No books found in the latest upload section")
            return

        if self.book_ids[-1] <= self.last_id_scraped:
            # No need to continue
            logger.info("Already have the newest book")
            return

        # Log how many items in total we will be parsing
        scraper.stats['ref_data_count'] = len(self.book_ids)

        # Only ever use 1 thread here
        self.thread_profile(1, 'requests', self.book_ids, Worker)

    def get_latest_books(self):
        """
        Get the latest uploaded book id's and return as a list
        """
        logger.info("Get latest upload ids")

        tmp_web = Web(self, 'requests')

        # Get the json data
        try:
            soup = tmp_web.get_site(self.BASE_URL, page_format='html')
        except:
            logger.critical("Problem loading home page to get latest uploads", exc_info=True)
            sys.exit(1)

        book_list_raw = soup.find_all("td", {"width": 120})
        book_list = []
        for book in book_list_raw:
            try:
                book_id_raw = book.find('a').get('href').split('/')[2]
                book_list.append(int(book_id_raw))
            except ValueError:
                logger.error("Could not get book id from {book_id_raw}".format(book_id_raw=book_id_raw))

        book_list.sort()
        return book_list

    def get_last_scraped(self):
        """
        Get last book scraped
        """
        last_scraped_id = db_session.query(Setting).filter(Setting.bit == 0).one().book_last_id

        if last_scraped_id is None:
            last_scraped_id = 0

        return last_scraped_id

    def log_last_scraped(self):
        try:
            try:
                last_book_id = db_session.query(Book).order_by(Book.book_id.desc()).first()
                if last_book_id is not None:
                    setting = db_session.query(Setting).filter(Setting.bit == 0).one()
                    setting.book_last_id = last_book_id.book_id
                    setting.book_last_ran = cutil.get_datetime()

                    db_session.add(setting)
                    db_session.commit()
            except NoResultFound:
                # If there is no raw data then no books were collected
                pass

        except:
            logger.exception("Problem logging last book scraped")

    def insert_data(self, data):
        """
        Will handle inserting data into the database
        """
        try:
            # Check if book is in database, if so update else create
            try:
                book = db_session.query(Book).filter(Book.book_id == data.get('book_id')).one()
            except NoResultFound:
                book = Book()

            book.title = data.get('title')
            book.subtitle = data.get('subtitle')
            book.author = data.get('author')
            book.year = data.get('year')
            book.pages = data.get('pages')
            book.language = data.get('language')
            book.publisher = data.get('publisher')
            book.isbn = data.get('isbn')
            book.format = data.get('format')
            book.description = data.get('description')
            book.file_source = data.get('file_source')
            book.file_cover_source = data.get('file_cover_source')
            book.file_location = data.get('file_location')
            book.file_cover_location = data.get('file_cover_location')
            book.book_id = data.get('book_id')
            book.time_collected = data.get('time_collected')

            db_session.add(book)
            db_session.commit()
            # self.track_stat('rows_added_to_db', rows_affected)

        except Exception:
            db_session.rollback()
            logger.exception("Error adding to db {data}".format(data=data))


def sigint_handler(signal, frame):
    logger.critical("Keyboard Interrupt")
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)

    try:
        scraper = ItEbooks()
        try:
            scraper.start()
            scraper.cleanup()

        except Exception:
            logger.critical("Main Error", exc_info=True)

    except Exception:
        logger.critical("Setup Error", exc_info=True)

    finally:
        scraper.log_last_scraped()
        try:
            # Log stats
            scraper_monitor.stop(total_urls=scraper.stats['total_urls'],
                                 ref_data_count=scraper.stats['ref_data_count'],
                                 ref_data_success_count=scraper.stats['ref_data_success_count'],
                                 rows_added_to_db=scraper.stats['rows_added_to_db'])

        except NameError:
            # If there is an issue with scraper.stats
            scraper_monitor.stop()

        except Exception:
            logger.critical("Scraper Monitor Stop Error", exc_info=True)
            scraper_monitor.stop()

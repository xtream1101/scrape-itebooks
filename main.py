import os
import re
import sys
import yaml
import signal
import argparse
from custom_utils.custom_utils import CustomUtils
from custom_utils.exceptions import *
from custom_utils.sql import *

# Set timezone to UTC
os.environ['TZ'] = 'UTC'


class ItEbooks(CustomUtils):

    def __init__(self, base_dir, restart=False, proxies=[], url_header=None):
        super().__init__()
        # Make sure base_dir exists and is created
        self._base_dir = base_dir

        # Do we need to restart
        self._restart = restart

        # Set url_header
        self._url_header = self.set_url_header(url_header)

        # If we have proxies then add them
        if len(proxies) > 0:
            self.set_proxies(proxies)
            self.log("Using IP: " + self.get_current_proxy())

        # Setup database
        self._db_setup()

        # Start parsing the site
        self.start()

    def start(self):
        latest = self.get_latest()

        if self._restart is True:
            progress = 0
        else:
            progress = self.sql.get_progress()

        if latest == progress:
            # Nothing new to get
            self.cprint("Already have the latest")
            return

        for i in range(progress + 1, latest + 1):
            self.cprint("Getting eBook: " + str(i))
            if self._restart is True:
                check_data = self._db_session.query(Data).filter(Data.id == i).first()
                if check_data is not None:
                    continue

            if self.parse(i) is not False:
                self.sql.update_progress(i)

    def get_latest(self):
        """
        Parse `http://it-ebooks.info/` and get the id of the newest book
        :return: id of the newest item
        """
        self.cprint("##\tGetting newest upload id...\n")

        url = "http://it-ebooks.info/"
        # get the html from the url
        try:
            soup = self.get_site(url, self._url_header)
        except RequestsError as e:
            print("Error getting latest: " + str(e))
            sys.exit(0)

        max_id = soup.find("td", {"width": 120}).find("a")['href'].split('/')[-2]
        self.cprint("##\tNewest upload: " + max_id + "\n")
        return int(max_id)

    def parse(self, id_):
        """
        Using BeautifulSoup, parse the page for the wallpaper and its properties
        :param id_: id of the book on `http://it-ebooks.info/book/`
        :return:
        """
        prop = {}
        prop['id'] = str(id_)

        url = "http://it-ebooks.info/book/" + prop['id']
        # get the html from the url
        try:
            soup = self.get_site(url, self._url_header)
        except RequestsError as e:
            print("Error getting (" + url + "): " + str(e))
            return False

        # Check for 404 page, not caught in get_html because the site does not throw a 404 error
        if soup.find("img", {"alt": "Page Not Found"}):
            # Users do not need to know about the 404 errors
            # self.log("Error [parse]: 404 " + url)
            return False

        # Find data
        prop['cover_img'] = "http://it-ebooks.info" + soup.find("img", {"itemprop": "image"})['src'].strip()
        prop['title'] = soup.find("h1", {"itemprop": "name"}).getText().strip()
        prop['description'] = soup.find("span", {"itemprop": "description"}).getText().strip()
        prop['publisher'] = soup.find(attrs={"itemprop": "publisher"}).getText().strip()
        prop['author'] = soup.find(attrs={"itemprop": "author"}).getText().strip().split(', ')
        prop['isbn'] = soup.find(attrs={"itemprop": "isbn"}).getText().strip()
        prop['year'] = soup.find(attrs={"itemprop": "datePublished"}).getText().strip()
        prop['pages'] = soup.find(attrs={"itemprop": "numberOfPages"}).getText().strip()
        prop['language'] = soup.find(attrs={"itemprop": "inLanguage"}).getText().strip()
        prop['format'] = soup.find(attrs={"itemprop": "bookFormat"}).getText().strip().lower()
        try:
            prop['dl_link'] = soup.find("a", {"href": re.compile('http://filepi.com')})['href']
        except TypeError:
            # Download link is not there
            return False

        # sanitize data
        prop['publisher'] = self.sanitize(prop['publisher'])
        prop['title'] = self.sanitize(prop['title'])

        # Download images and save
        file_name = prop['publisher'] + " - " + prop['title']
        file_ext_cover = self.get_file_ext(prop['cover_img'])

        path_title = prop['title']
        if len(path_title) > 32:
            path_title = path_title[0:32] + "---"

        book_base_dir = os.path.join(self._base_dir,
                                     "ebooks",
                                     prop['publisher'],
                                     path_title
                                     )
        prop['save_path'] = os.path.join(book_base_dir,
                                         file_name + file_ext_cover
                                         )
        prop['save_path_cover'] = os.path.join(book_base_dir,
                                               file_name + "." + prop['format']
                                               )

        prop['rel_path'] = prop['save_path'].replace(self._base_dir, "")
        prop['rel_cover_path'] = prop['save_path_cover'].replace(self._base_dir, "")

        self._url_header['Referer'] = url

        cover_dl = self.download(prop['cover_img'], prop['save_path_cover'], self._url_header)
        book_dl = self.download(prop['dl_link'], prop['save_path'], self._url_header)

        # Only save in database if book file was saved
        if book_dl:
            self._save_meta_data(prop)

        # Everything was successful
        return True

    def _save_meta_data(self, data):

        itebook_data = Data(id=data['id'],
                            rel_path=data['rel_path'],
                            rel_cover_path=data['rel_cover_path'],
                            cover_img=data['cover_img'],
                            description=data['description'],
                            dl_link=data['dl_link'],
                            book_format=data['format'],
                            isbn=data['isbn'],
                            language=data['language'],
                            pages=data['pages'],
                            publisher=data['publisher'],
                            title=data['title'],
                            year=data['year'],
                            )

        self._db_session.add(itebook_data)

        try:
            self._db_session.commit()
        except sqlalchemy.exc.IntegrityError:
            # tried to add an item to the database which was already there
            pass

        # Save tags in their own table
        self._save_author_data(data['author'], data['id'])

    def _save_author_data(self, authors, data_id):
        for author in authors:
            itebook_author = Author(name=author,
                                    )
            self._db_session.add(itebook_author)
            self._db_session.flush()

            itebook_data_author = DataAuthor(author_id=itebook_author.id,
                                             data_id=data_id,
                                             )
            self._db_session.add(itebook_data_author)

        try:
            self._db_session.commit()
        except sqlalchemy.exc.IntegrityError:
            # tried to add an item to the database which was already there
            pass

    def _db_setup(self):
        # Version of this database
        db_version = 1
        db_file = os.path.join(self._base_dir, "it-ebooks.sqlite")
        self.sql = Sql(db_file, db_version)
        is_same_version = self.sql.set_up_db()
        if not is_same_version:
            # Update database to work with current version
            pass

        # Get session
        self._db_session = self.sql.get_session()


class Author(Base):
    __tablename__ = 'authors'
    id   = Column(Integer,     primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)


class Data(Base):
    __tablename__ = 'data'
    id             = Column(Integer,     primary_key=True)
    rel_path       = Column(String(300), nullable=False)
    rel_cover_path = Column(String(300), nullable=False)
    cover_img      = Column(String(200), nullable=False)
    description    = Column(String,      nullable=False)
    dl_link        = Column(String(200), nullable=False)
    book_format    = Column(String(10),  nullable=False)
    isbn           = Column(String(20),  nullable=False)
    language       = Column(String(20),  nullable=False)
    pages          = Column(Integer,     nullable=False)
    publisher      = Column(String(100), nullable=False)
    title          = Column(String(200), nullable=False)
    year           = Column(Integer,     nullable=False)


class DataAuthor(Base):
    __tablename__ = 'data_authors'
    author_id = Column(Integer, ForeignKey(Author.id))
    data_id   = Column(Integer, ForeignKey(Data.id))
    __table_args__ = (
            PrimaryKeyConstraint('author_id', 'data_id'),
            )


def signal_handler(signal, frame):
    print("")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    # Deal with args
    parser = argparse.ArgumentParser(description='Scrape site and archive data')
    parser.add_argument('-c', '--config', help='Config file')
    parser.add_argument('-d', '--dir', help='Absolute path to save directory')
    parser.add_argument('-r', '--restart', help='Set to start parsing at 0', action='store_true')
    args = parser.parse_args()

    # Set defaults
    save_dir = None
    restart = None
    proxy_list = []

    if args.config is not None:
        # Load config values
        if not os.path.isfile(args.config):
            print("No config file found")
            sys.exit(0)

        with open(args.config, 'r') as stream:
            config = yaml.load(stream)

        # Check config file first
        if 'save_dir' in config:
            save_dir = config['save_dir']
        if 'restart' in config:
            restart = config['restart']

        # Proxies can only be set via config file
        if 'proxies' in config:
            proxy_list = config['proxies']

    # Command line args will overwrite config args
    if args.dir is not None:
        save_dir = args.dir

    if restart is None or args.restart is True:
        restart = args.restart

    # Check to make sure we have our args
    if args.dir is None and save_dir is None:
        print("You must supply a config file with `save_dir` or -d")
        sys.exit(0)

    save_dir = CustomUtils().create_path(save_dir, is_dir=True)

    # Start the scraper
    scrape = ItEbooks(save_dir, restart=restart, proxies=proxy_list)

    print("")

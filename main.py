import os
import re
import sys
import signal
from custom_utils.custom_utils import CustomUtils
from custom_utils.exceptions import *
from custom_utils.sql import *

# Set timezone to UTC
os.environ['TZ'] = 'UTC'


class ItEbooks(CustomUtils):

    def __init__(self, base_dir, url_header=None):
        super().__init__()

        # Make sure base_dir exists and is created
        self._base_dir = base_dir

        # Set url_header
        self._url_header = self._set_url_header(url_header)

        # Setup database
        self._db_setup()

        # Start parsing the site
        self.start()

    def start(self):
        latest = self.get_latest()
        progress = self.sql.get_progress()

        if latest == progress:
            # Nothing new to get
            self.cprint("Already have the latest")
            return

        for i in range(progress + 1, latest + 1):
            self.cprint("Getting eBook: " + str(i))
            self.parse(i)
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
        except RequestsError:
            # TODO: Do something more useful here i.e. let the user know and do not just start at 0
            return 0
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
        except RequestsError:
            # TODO: give a better error
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

    def _set_url_header(self, url_header):
        if url_header is None:
            # Use default from CustomUtils
            return self.get_default_header()
        else:
            return url_header

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
    if len(sys.argv) < 2:
        print("You must pass in the save directory of the scraper")

    save_dir = CustomUtils().create_path(sys.argv[1], is_dir=True)
    # Start the scraper
    scrape = ItEbooks(save_dir)

    print("")

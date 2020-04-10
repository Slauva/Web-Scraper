from bs4 import BeautifulSoup
from requests import get
from user_agent import generate_user_agent
from re import compile
from aiohttp import ClientSession
from time import time
from datetime import datetime
import asyncio
import sys
import csv


def run(coroutine):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coroutine)
    asyncio.set_event_loop(None)
    loop.close()


if not sys.version_info >= (3, 7):
    asyncio.create_task = asyncio.ensure_future
    asyncio.gather = asyncio.wait
    asyncio.run = run


class Scraper:
    TODAY = datetime.now()

    MONTHS = {
        'января': '01',
        'февраля': '02',
        'марта': '03',
        'апреля': '04',
        'мая': '05',
        'июня': '06',
        'июля': '07',
        'августа': '08',
        'сентября': '09',
        'октября': '10',
        'ноября': '11',
        'декабря': '12',
    }

    def __init__(self, url: str, event_selector: str, group_selector=None, break_selector=None):
        self.eSelector = event_selector
        self.headers = self.getHeaders()
        self.break_selector = break_selector
        self.URL = self.getBasicURL(url)
        self.event_links = list()
        self.events = list()

    def getBasicURL(self, url):
        r = url.split('/')
        return '/'.join(r[:3])

    def getMonth(self, month):
        month = month.lower()
        r = Scraper.MONTHS.get(month)
        return r if r != None else month

    def getHeaders(self):
        return {'User-Agent': generate_user_agent(device_type='desktop', os=('mac', 'linux'))}

    def getPage(self, url: str):
        return get(url, headers=self.headers).text

    def isCorrectURL(self, url: str):
        """
            Integrity Link Check \n
            /event/555/ -> Incorrect(False) \n
            https://example.com/event/555/ -> Correct(True)
        """
        pattern = compile('http')
        return pattern.match(url) != None

    def getLinks(self, soup: BeautifulSoup, selector: str, break_selector: str = None):
        """
            Function get all links from the page by class selector
        """
        _a = soup.select(selector)

        if break_selector != None:
            _a = list(filter(lambda a: break_selector[1:] not in a.parent.get('class'), _a))

        if _a == []:
            return []
        if self.isCorrectURL(_a[0]['href']):
            return [a['href'] for a in _a]
        else:
            return [self.URL + a['href'] for a in _a]

    def formatData(self, data):
        r = data
        if type(r) != list:
            r = r.split()
        r[-2] = self.getMonth(r[-2])
        if len(r[0]) == 1: r[0] = '0' + r[0]
        return '.'.join([r[0], r[-2], r[-1]])

    def fixText(self, string: str):
        """
            Clean all spaces and special chars(\\n\\t\\r...)
        """
        return ' '.join(string.strip().split())

    def checkURLRepeat(self, urls: list):
        """
            Clear all url repeating in the list
        """
        return list(set(urls))

    def getEventsByGroup(self, url: str, group_selector: str, pattern: str = '/events/'):
        """
            This function is needed if all events are distributed by group, who conducts them or categories. \n
            As a result, it will return an array of links by which the scraper will search for events \n
            :param pattern needed to complete links with events like https://example.com + /events/

            ! This function does not work with multi pages websites. \n
            ! If website has pagination then the function will not scrape all pages, only current.
        """
        page = self.getPage(url)
        soup = BeautifulSoup(page, 'lxml')
        groups = self.getLinks(soup, group_selector, self.break_selector)
        return [link[:-1] + pattern for link in groups]

    async def fetch(self, url, session):
        async with session.get(url) as response:
            return await response.text()

    async def spider(self, url, session):
        page = await self.fetch(url, session)
        soup = BeautifulSoup(page, 'lxml')
        self.events.append(self.getInformation(soup, url))

    async def takeLinks(self, url, session, selector):
        page = await self.fetch(url, session)
        soup = BeautifulSoup(page, 'lxml')
        result = self.getLinks(soup, selector, self.break_selector)
        return result

    async def scrape(self, urls: list):
        """
            Function like scraper but it has some differences:
            1) The function can't scrape pages with pagiantion
            2) The function throw BeautifulSoup object in function getInformation, be careful
            3) If you want to scrape only one page you must send array with one url, it's important
        """
        # GET all links on the events
        tasks = []
        async with ClientSession(headers=self.headers) as session:
            for url in urls:
                task = asyncio.create_task(self.takeLinks(url, session, self.eSelector))
                tasks.append(task)
            if sys.version_info >= (3, 7):
                done = await asyncio.gather(*tasks)
            else:
                done, _ = await asyncio.gather(tasks, return_when=asyncio.ALL_COMPLETED)


        for link in done:
            if not sys.version_info >= (3, 7):
                self.event_links.extend(link.result())
            else:
                self.event_links.extend(link)

        print('Step 1 has finished', end='\r')

        if self.event_links == []: return []

        # Scrape all events
        tasks = []
        async with ClientSession(headers=self.headers) as session:
            for event in self.event_links:
                task = asyncio.create_task(self.spider(event, session))
                tasks.append(task)

            if sys.version_info >= (3, 7):
                await asyncio.gather(*tasks)
            else:
                await asyncio.gather(tasks)

        print('Step 2 has finished', end='\r')
        print(' ' * 100, end='\r')
        print(self.__class__.__name__, ': success')

    def save(self, path='events.csv'):
        with open(path, 'w', encoding='utf-8-sig', newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            header = ['Title', 'Description', 'Date', 'Time', 'Address', 'Link']
            writer.writerow(header)
            for line in self.events:
                writer.writerow(line)
        print(self.__class__.__name__, f': saved by name <{path}>')

    def getInformation(self, soup: BeautifulSoup, url: str):
        raise NotImplementedError('Function getInformation has not been creating')


class MeetUp(Scraper):
    def __init__(self, url: str, event_selector: str, group_selector=None):
        super().__init__(url, event_selector, group_selector=group_selector)

        groups = self.getEventsByGroup(url, group_selector, pattern='/events/')
        asyncio.run(self.scrape(groups))

    def getInformation(self, soup: BeautifulSoup, url: str):
        title = soup.select_one('h1').text
        try:
            descr = soup.select_one('.event-description.runningText > p').text
        except:
            descr = ''
        time = soup.select_one('.eventTimeDisplay > time').text.split()
        if len(time) > 8:
            t = time[5]
            d1, d2 = time[1:4], time[8:11]
            date, to_date = map(self.formatData, [d1, d2])
            _time = t
        else:
            date = self.formatData(time[1:4])
            _time = time[-4].replace('г.', '') + ' - ' + time[-2]
        try:
            address = soup.select_one('address').text
        except:
            address = ''
        return [self.fixText(x) for x in [title, descr, date, _time, address, url]]


class Leader_ID(Scraper):

    def __init__(self, url: str, event_selector: str):
        super().__init__(url, event_selector)
        asyncio.run(self.scrape([url]))

    def getInformation(self, soup: BeautifulSoup, url: str):
        title = soup.select_one('h2.article-header__title').text
        descr = soup.select_one('p.article__intro.mb-5').text
        info = soup.select('.article__sidebar-content.aside__content > p')
        data = ' '.join(info[2].text.split()[1:])
        data = self.formatData(data)
        time = ' '.join(info[3].text.split()[1:])
        address = ' '.join(info[4].text.split()[1:])
        return list(map(self.fixText, [title, descr, data, time, address, url]))


class TimePad(Scraper):

    def __init__(self, urls: list, event_selector: str, break_selector: str):
        super().__init__(urls[0], event_selector, break_selector=break_selector)
        asyncio.run(self.scrape(urls))

    def parseTime(self, data):
        data = data.split()
        year = TimePad.TODAY.year if len(data) != 7 else data[2]
        if TimePad.MONTHS.get(data[1].lower()) != None:
            month = TimePad.MONTHS.get(data[1].lower())
            if len(data[0]) == 1: data[0] = '0' + data[0]
            day = f'{data[0]}.{month}.{year}'
        else:
            month = data[1]
            if len(data[0]) == 1: data[0] = '0' + data[0]
            day = f'{data[0]} {month} {year}'
        time = f'{data[-3]} - {data[-1]}'
        return [day, time]

    def getInformation(self, soup: BeautifulSoup, url: str):
        title = soup.select_one('head > title').text
        try:
            descr = soup.find('meta', name='description')['content']
        except:
            descr = soup.find('meta', property="og:description")['content']

        try:
            buf = soup.select_one('.ep3-pagesummary__time-begin > span').text.strip().replace('\xa0', ' ')
            if len(buf.split()) > 7:
                buf = buf.split()
                try:
                    yy = int(buf[-1])
                except:
                    yy = TimePad.TODAY.year
                time = buf[1]
                if len(buf[2]) == 1: buf[2] = '0' + buf[2]
                day = buf[2]
                month = buf[3]
                data = f'{day}.{self.getMonth(month)}.{yy}'
            else:
                buf = self.parseTime(buf)
                data = buf[0]
                time = buf[1]
            address = soup.select_one('.ep3-pagesummary__place-adress > span').text.strip()
        except:
            event_info = soup.select('.mcards > div')
            dataSet = event_info[0].select_one('.tcaption.tcaption--block').text.strip().split()[3:]
            addressSet = event_info[1].select_one('.tcaption.tcaption--block').text.strip().split()[:-3]

            ind = dataSet.index('Добавить')
            dataSet = dataSet[:ind]

            time = dataSet[-1].split('–')
            time = f'{time[0]} - {time[1]}'
            if len(dataSet[0]) == 1: dataSet[0] = '0' + dataSet[0]
            if len(dataSet) == 4:
                data = f'{dataSet[0]}.{self.getMonth(dataSet[1].lower())}.{dataSet[2]}'
            else:
                data = f'{dataSet[0]}.{self.getMonth(dataSet[1].lower())}.{TimePad.TODAY.year}'
            address = ' '.join(addressSet)

        return [self.fixText(x) for x in [title, descr, data, time, address, url]]


if __name__ == "__main__":
    t0 = time()
    leaderid = Leader_ID('https://leader-id.ru/events/?CityIds%5B%5D=887', event_selector='.event.overflow_event > a')
    meetup = MeetUp('https://www.meetup.com/ru-RU/find/tech/?allMeetups=false&radius=150&userFreeform=Казань&mcId=c1036275&mcName=Казань%2C+RU&sort=founded_date', event_selector='div > a.eventCard--link', group_selector='li > div > a.display-none')
    timepad = TimePad(["https://timepad.ru/afisha/naberezhnye-chelny/search/it/all/", "https://timepad.ru/afisha/kazan/search/it/all/", "https://timepad.ru/afisha/innopolis/search/it/all/"], event_selector='.meventcard > a', break_selector='.meventcard--passed')
    # =====<Save>=====
    leaderid.save(path='results/leaderId.csv')
    meetup.save(path='results/meetUp.csv')
    timepad.save(path='results/timepad.csv')
    print(time() - t0, flush=True)
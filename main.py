import asyncio
import aiofiles
import aiohttp
import async_timeout
import argparse
import logging
import os
import time
from bs4 import BeautifulSoup




LOGGER_FORMAT = '%(asctime)s %(message)s'

parser = argparse.ArgumentParser(
    description='Calculate the number of comments of the top stories in HN.')
parser.add_argument(
    '--period', type=int, default=5, help='Number of seconds between poll')
parser.add_argument(
    '--limit', type=int, default=5,
    help='Number of new stories to calculate comments for')
parser.add_argument(
    '--dir', type=str, help='Directry of news to save'
)
parser.add_argument('--verbose', action='store_true', help='Detailed output')


logging.basicConfig(format=LOGGER_FORMAT, datefmt='[%H:%M:%S]')
log = logging.getLogger()
log.setLevel(logging.INFO)


class ConnectExceedNumber(Exception):
    pass

class AsyncCrawler:
    """Provides counting of URL fetches for a particular task.
    """
    def __init__(self, dir_base):
        self.state_url = "https://news.ycombinator.com/"
        self.concurrency = 3
        self.fetch_timeout = 10
        self.fetch_counter = 0
        self.max_fetch = 5
        self.temp_url = "item?id={}"
        self.seen_ids = set()
        self.dir = dir_base

    async def fetch(self, session, url):
        """Fetch a URL using aiohttp returning parsed JSON response.
        As suggested by the aiohttp docs we reuse the session.
        """
        async with asyncio.Semaphore(self.concurrency):
            async with async_timeout.timeout(self.fetch_timeout):
                self.max_fetch += 1
                if self.max_fetch > self.max_fetch:
                    raise ConnectExceedNumber('Number of connection attempts hit limit {}'.format(self.max_fetch))
                try:
                    log.info('Scraping %s', url)

                    async with session.get(url) as response:
                        return await response.text()
                except Exception as e:
                    log.error("Error retrieving {}: {}".format(url, e))
                    raise

    async def load_save_data(self, session, dir_id, url):
        base_dir, ind = dir_id
        if ind == 0:
            file_name = 'base_link.html'
        else:
            file_name = 'link_{}.html'.format(str(ind))
        save_dir = os.path.join(base_dir, file_name)
        log.info('File saved in %s', save_dir)
        try:
            response = await self.fetch(session, url)
        except Exception as e:
            log.error("Error saving comments links: {}".format(e))
            raise
        async with aiofiles.open(save_dir, 'w', encoding='utf8') as file:
            await file.write(response)

    async def find_comments_url(self, loop, session, post):
        """Retrieve data for current post and recursively for all comments.
        """
        start_time = time.time()
        parent_el = post.find('a', class_='titlelink')
        post_id = post['id']
        url_check = self.temp_url.format(post_id)
        url_comment = (self.state_url+self.temp_url).format(post_id)
        if url_check == parent_el['href']:
            urls = [url_comment]
        else:
            urls = [parent_el['href']]

        try:
            response = await self.fetch(session, url_comment)
        except Exception as e:
            log.error("Error retrieving comments from post {}: {}".format(post_id, e))
            raise
        response_res = BeautifulSoup(response, 'html.parser').find_all('span', class_='commtext c00')

        for comm in response_res:
            link = comm.find_all('a')
            for li in link:
                urls.append(li['href'])

        dir_new = os.path.join(self.dir, post_id)
        os.makedirs(dir_new)
        log.info("Post {} has {} comments".format(post_id, len(urls)-1))
        comments_url = [loop.create_task(self.load_save_data(session, (dir_new, ind), url))
                        for ind, url in enumerate(urls)]
        try:
            await asyncio.gather(*comments_url)
        except Exception as e:
            log.error("Error saving comments for story {}: {}".format(post_id, e))
            raise
        return time.time()-start_time

    async def get_comments_of_top_stories(self, loop, session, limit):
        """Retrieve top stories in HN.
        """
        try:
            response = await self.fetch(session, self.state_url)
        except Exception as e:
            log.error("Error retrieving top stories: {}".format(e))
            raise

        response_res = BeautifulSoup(response, 'html.parser').find_all('tr', class_='athing')
        tasks = []
        for post in response_res[:limit]:
            post_id = post['id']
            if post_id in self.seen_ids:
                continue
            self.seen_ids.add(post_id)
            tasks.append(loop.create_task(self.find_comments_url(loop, session, post)))

        try:
            results = await asyncio.gather(*tasks)
        except Exception as e:
            log.error("Error retrieving comments for top stories: {}".format(e))
            raise

        return sum(results)

    async def poll_top_stories_for_comments(self, loop, session, period, limit):
        """Periodically poll for new stories and retrieve number of comments.
        """
        iteration = 1
        errors = []
        while True:
            if errors:
                log.info('Error detected, quitting')
                return

            log.info("Calculating comments for top {} stories. ({}) ".format(limit, iteration))

            future = loop.create_task(
                self.get_comments_of_top_stories(loop, session, limit))

            def callback(fut):
                try:
                    fetch_time = fut.result()
                except Exception as e:
                    log.exception('Unexpected error')
                    errors.append(e)
                else:
                    log.info(
                        '> Calculating comments took {:.2f} seconds'.format(fetch_time))

            future.add_done_callback(callback)

            log.info("Waiting for {} seconds...".format(period))
            iteration += 1
            if iteration > 5:
                break
            await asyncio.sleep(period)


async def main(args):
    loop = asyncio.get_event_loop()
    crawler = AsyncCrawler(args.dir)
    async with aiohttp.ClientSession() as session:
        await asyncio.Task(crawler.poll_top_stories_for_comments(loop, session, args.period, args.limit))


if __name__ == '__main__':
    args = parser.parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
    loop.close()

import sys
import time
from selenium_base import driver_creation
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

headings = ['Listing code', 'Brand', 'Model', 'Reference number', 'Dealer product code', 'Movement',
            'Bracelet material', 'Year of production', 'Condition', 'Scope of delivery', 'Gender',
            'Location', 'Price', 'Availability', 'Movement/Caliber', 'Base caliber', 'Power reserve',
            'Number of jewels', 'Case material', 'Case diameter', 'Water resistance', 'Bezel material', 'Crystal',
            'Dial', 'Dial numerals', 'Bracelet color', 'Clasp', 'Clasp material', 'Functions',
            'Other', 'url_reference']

watch_records = pd.DataFrame(columns=headings)
COUNT = 0


def watch_collection(maker):
    driver = driver_creation(is_headless=True)

    watches = []

    for i in tqdm(range(1, 6)):
        url = f'https://www.chrono24.ca/{maker}/index.htm?man={maker}&pageSize=120&showpage={i}'
        driver.get(url)

        try:
            privacy = driver.find_element(By.XPATH, '/html/body/div[11]/div/div/div[2]/button')
            privacy.click()
        except NoSuchElementException:
            pass

        watch_links = driver.find_elements(By.CLASS_NAME, 'block-item')
        watch_links = [elem.get_attribute('href') for elem in watch_links]
        watches.extend(watch_links)

        i += 1

    with open(f'watches/{maker}.txt', 'w+') as f:
        for line in watches:
            f.write(f"{line}\n")


def main():
    workers = 12

    test = 'watches/models_makers.txt'

    with open(test) as f:
        urls = f.read().splitlines()

    urls = list(dict.fromkeys(urls))

    print(f'Number of listings -> {len(urls)}')

    urls = list(split(urls, workers))


    drivers = [driver_creation(is_headless=True) for _ in range(workers)]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(watch_information, urls, drivers)
    [driver.quit() for driver in drivers]


def watch_information(urls, driver):
    global watch_records
    global COUNT
    for watch in urls:
        record = pd.Series(dict(zip(headings, [None] * len(headings))))

        driver.get(watch)

        if driver.current_url != watch:
            continue

        try:
            privacy = driver.find_element(By.XPATH, '/html/body/div[13]/div/div/div[2]/button')
            privacy.click()
        except NoSuchElementException:
            pass

        table = driver.find_element(By.CSS_SELECTOR,
                                    'div.js-tab:nth-child(2) > section:nth-child(1) > div:nth-child(1) > div:nth-child(1) > table:nth-child(1)')

        bodies = table.find_elements(By.TAG_NAME, 'tbody')

        supplemental_information = 0

        for body in bodies:
            rows = body.find_elements(By.TAG_NAME, 'tr')

            for row in rows[1:]:
                dim = row.find_elements(By.TAG_NAME, 'td')
                value = dim[-1].accessible_name
                article = dim[0].accessible_name

                if len(dim) == 1 and supplemental_information == 0:
                    record.loc['Functions'] = value
                    supplemental_information += 1
                    pass

                elif len(dim) == 1 and supplemental_information == 1:
                    record.loc['Other'] = value
                    pass

                else:
                    if article in headings:
                        record.loc[article] = value

        record.loc['url_reference'] = watch

        # watch_records = pd.concat([watch_records, record.to_frame().T], ignore_index=True)
        record.to_frame().T.to_csv('watches/watch_records_compiled.csv', index=False, header=False, mode='a')

        if COUNT % 1000 == 0:
            print(COUNT)
        COUNT += 1


def split(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out


if __name__ == '__main__':
    main()

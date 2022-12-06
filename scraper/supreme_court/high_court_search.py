'''This code helps to search Indian High courts website https://judgments.ecourts.gov.in/pdfsearch and download the pdfs and convert them to text'''

from typing import Optional
import datetime
import copy
import datetime
import random
import os
from selenium import webdriver
import time
import pandas as pd
import re
import requests
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import string
from joblib import Parallel, delayed
from tqdm import tqdm
import PyPDF2
from haystack.nodes.file_converter.pdf import PDFToTextConverter
from PIL import Image
from pdf_to_text.pdf_to_text_converter import read_one_pdf_file_convert_to_txt_and_write
from pytesseract import image_to_string

class HighCourtSearch:
    def __init__(self,
                 output_folder_path: str,
                 high_court_name: list[str],
                 search_date_range: tuple[datetime.date],
                 court_bench: Optional[list[str]] = 'all',
                 case_type: Optional[list[str]] = None,
                 case_type_regex: Optional[str] = None,
                 disposal_nature: Optional[list[str]] = None,
                 disposal_nature_regex: Optional[str] = None,

                 ):
        """

        :param high_court_name: Names of the high courts to search for
        :param search_date_range: Time range of judgment dates to search for
        :param court_bench: Specify the exact names of the bench. Default value 'all' will pick up all the benches of a given high court
        :param case_type: If specified then only those case types are used for searching. These vary by high court and bench.
        Please refer to the high court websites to get details.
        If both case_type and case_type_regex are provided then any case type than matches either case_type or case_type_regex are used for searching.
        :param case_type_regex: Regex pattern to match the case types. E.g. "Crl."
        :param disposal_nature: Disposal Natures to filter. These vary by court and bench. If both disposal_nature and disposal_nature_regex are specified then union of them is considered
        :param disposal_nature_regex: Regrex pattern to match the Disposal Natures
        """
        self.high_court_name =high_court_name
        self.search_date_range = search_date_range
        self.court_bench = court_bench
        self.case_type = case_type
        self.case_type_regex = case_type_regex
        self.disposal_nature = disposal_nature
        self.disposal_nature_regex = disposal_nature_regex
        self.hc_homepage = 'https://judgments.ecourts.gov.in/pdfsearch'

        self.output_folder_path = output_folder_path
        os.makedirs(self.output_folder_path, exist_ok=True)

    def get_captha_text(self,driver):
        driver.save_screenshot('screenshot.png')
        element = driver.find_element('id', "captcha_image")
        location = element.location
        size = element.size

        im = Image.open('screenshot.png')
        left = location['x']
        top = location['y']
        right = location['x'] + size['width']
        bottom = location['y'] + size['height']
        im = im.crop((left, top, int(right), int(bottom)))

        im.save('new.png')
        captcha_text = image_to_string(im, config='digits')
        im.close()
        captcha_text = re.sub(r'[^0-9]', '', captcha_text)
        return captcha_text

    def refresh_captha(self,driver):
        driver.find_element('xpath', '/html/body/div[2]/main/form/div[3]/div[1]/a/img').click()  ## refresh captcha

    def go_to_advanced_search(self,driver):
        captha_retry_cnt = 5
        captcha_text = self.get_captha_text(driver)
        while len(captcha_text)!= 6:
            self.refresh_captha(driver)
        driver.find_element("id", "captcha").send_keys(captcha_text)
        driver.find_element("link text", 'Advanced Search').click()
        success = False
        while captha_retry_cnt>0:
            try:
                if driver.find_element('id', "errorIcon").is_displayed():
                    time.sleep(2)
                    driver.find_element(By.CLASS_NAME, "btn-close").click()
                    time.sleep(2)
                    self.refresh_captha(driver)
                    time.sleep(1)
                    captcha_text = self.get_captha_text(driver)
                    driver.find_element("id", "captcha").clear()
                    driver.find_element("id", "captcha").send_keys(captcha_text)
                    driver.find_element("link text", 'Advanced Search').click()
                    captha_retry_cnt -=1
            except:
                success = True
        return success

    def parse_page(self,driver):
        result_elements = driver.find_element('id', 'report_body').find_elements('tag name', 'tr')
        result_details = []
        for result_element in result_elements:
            result_details_dict={}
            case_details_text = result_element.find_element(By.CLASS_NAME, 'caseDetailsTD').text
            case_details_list = case_details_text.split(" | ")
            for case_detail in case_details_list:
                key,val = case_detail.split(' : ',maxsplit=1)
                result_details_dict[key] = val

            case_name = result_element.find_element(By.CSS_SELECTOR,"button[id^=link]")
            result_details_dict['case_name'] = case_name.text
            case_name.click()
            judgment_url = driver.find_element('id', 'viewFiles-body').find_element('tag name', "object").get_attribute('data')
            result_details_dict['judgment_url']=judgment_url
            result_details.append(copy.deepcopy(result_details_dict))
        return result_details

    def search(self):
        driver = webdriver.Firefox()
        driver.get(self.hc_homepage)
        time.sleep(1)

        if self.go_to_advanced_search(driver):
            results_details = self.parse_page(driver)
            results_df = pd.DataFrame.from_records(results_details)
            results_df.to_csv(os.path.join(self.output_folder_path,'metadata.csv'),index=False)

if __name__=='__main__':
    output_folder_path = '/Users/prathamesh/tw_projects/OpenNyAI/data/court_search/hc/test'
    h = HighCourtSearch(output_folder_path=output_folder_path, high_court_name='Bombay',search_date_range=(datetime.date(2019,1,1),datetime.date(2020,1,1)))
    h.search()



'''This code helps to search Supreme Court of India website and download the pdfs and convert them to text'''
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
import hashlib
from pdf_to_text.pdf_to_text_converter import read_one_pdf_file_convert_to_txt_and_write


class SupremeCourtSearch:
    def __init__(self, search_type: str, search_kw: str,
                 output_folder_path: str,
                 search_date_range=(datetime.date(2022, 1, 1), datetime.date(2022, 12, 31))):
        self.sc_homepage = 'https://main.sci.gov.in/judgments'
        self.search_type = search_type  ##### 'actwise or free_text'
        self.search_date_range = search_date_range
        self.search_duration_one_year_intervals = self.create_one_year_time_ranges(search_date_range[0],
                                                                                   search_date_range[1])
        self.search_kw = search_kw

        self.output_folder_path = output_folder_path
        os.makedirs(self.output_folder_path, exist_ok=True)
        self.pdf_output_folder_path = os.path.join(output_folder_path, 'pdfs/')
        os.makedirs(self.pdf_output_folder_path, exist_ok=True)
        self.txt_output_folder_path = os.path.join(output_folder_path, 'txt/')
        os.makedirs(self.txt_output_folder_path, exist_ok=True)

        self.converter = PDFToTextConverter(remove_numeric_tables=True, valid_languages=["en"])

    def create_one_year_time_ranges(self, start_date: datetime.date, end_date: datetime.date) -> list:
        time_ranges = []
        time_range_start = start_date

        while time_range_start < end_date:
            time_range_end = time_range_start + datetime.timedelta(days=364)
            if time_range_end > end_date:
                time_range_end = end_date
            time_ranges.append([time_range_start, time_range_end])

            time_range_start = time_range_start + datetime.timedelta(days=365)

        return time_ranges

    def get_judgment_urls_by_searching(self) -> pd.DataFrame:
        search_results = pd.DataFrame()
        if self.search_type == 'actwise':
            for search_duration in self.search_duration_one_year_intervals:
                judgment_details = self.search_actwise(search_duration)
                search_results = pd.concat([search_results, judgment_details])

        elif self.search_type == 'free_text':
            search_results = self.search_free_text(self.search_date_range)
        else:
            print('Invalid search type. Choose between actwise or free_text')

        return search_results

    def search_actwise(self) -> pd.DataFrame:
        driver = webdriver.Firefox()
        driver.get(self.sc_homepage)
        time.sleep(2)
        driver.find_element('link text', 'Actwise').click()
        time.sleep(2)
        captcha = driver.find_element('id', 'cap').text.strip()
        driver.find_element("id", "ansCaptcha").send_keys(captcha)

        driver.find_element("id", "Jact_name").send_keys(self.search_kw)

        driver.find_element("id", "JBDfrom_date").clear()
        driver.find_element("id", "JBDfrom_date").send_keys(self.search_date_range[0].strftime('%d-%m-%Y'))

        driver.find_element("id", "JBDto_date").clear()
        driver.find_element("id", "JBDto_date").send_keys(self.search_date_range[1].strftime('%d-%m-%Y'))

        driver.find_element("id", "v_getJAW").click()
        time.sleep(5)
        # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'JAW')))

        result_table = driver.find_element('id', 'JAW').get_attribute('outerHTML')
        df = pd.read_html(result_table, extract_links='all')[0]
        df.columns = ['sr_no', 'attribute', 'value', 'link']

        df['sr_no'] = df['sr_no'].apply(lambda x: x[0])
        df['attribute'] = df['attribute'].apply(lambda x: x[0])
        df['value'] = df['value'].apply(lambda x: x[0])
        df['judgment_date'] = df['link'].apply(lambda x: x[0].split(' ')[0] if not pd.isnull(x) else x)
        df['language'] = df['link'].apply(
            lambda x: re.sub(r'[\(\)]', '', x[0].split(' ')[1]) if not pd.isnull(x) else x)
        df['judgment_url'] = df['link'].apply(lambda x: 'https://main.sci.gov.in' + x[1] if not pd.isnull(x) else x)

        df_reshaped = df.pivot(index='sr_no', columns='attribute', values='value')
        df_urls = df.groupby('sr_no')['judgment_url'].first()
        df_metadata = pd.concat([df_reshaped, df_urls], axis=1)
        driver.close()
        df_metadata.drop_duplicates(subset=['Case Number'], inplace=True)
        df_metadata['judgment_id'] = df_metadata['judgment_url'].apply(lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest())
        return df_metadata

    def search_free_text(self) -> pd.DataFrame:
        driver = webdriver.Firefox()
        driver.get(self.sc_homepage)
        time.sleep(1)
        driver.find_element('link text', 'Free Text').click()
        time.sleep(1)

        captcha = driver.find_element('id', 'cap').text.strip()
        driver.find_element("id", "ansCaptcha").send_keys(captcha)

        driver.find_element("id", "Free_Text").send_keys(self.search_kw)

        driver.find_element("id", "FT_from_date").clear()
        driver.find_element("id", "FT_from_date").send_keys(self.search_date_range[0].strftime('%d-%m-%Y'))

        driver.find_element("id", "FT_to_date").clear()
        driver.find_element("id", "FT_to_date").send_keys(self.search_date_range[1].strftime('%d-%m-%Y'))

        driver.find_element("id", "v_getTextFree").click()
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "get_free_text_data")))

        result_elements = driver.find_element('id', 'get_free_text_data').find_elements('tag name', 'option')
        result_details = []
        for i, result_element in enumerate(result_elements):
            try:
                result_data = {}
                party_names, judgment_date = result_element.text.split(' / ', maxsplit=1)
                split_party_names = party_names.split(' Vs ', maxsplit=1)
                if len(split_party_names) == 1:
                    petitioner_name = split_party_names[0]
                    respondent_name = ''
                else:
                    petitioner_name = split_party_names[0]
                    respondent_name = split_party_names[1]

                result_data['judgment_date'] = judgment_date
                result_data['petitioner_name'] = petitioner_name
                result_data['respondent_name'] = respondent_name

                result_element.click()

                try:
                    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.TAG_NAME, "textarea")))
                except:
                    #### Try clicking on previous element and click again
                    if i == 0 and len(result_elements) > 1:
                        result_elements[i + 1].click()
                        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.TAG_NAME, "textarea")))

                    else:
                        result_elements[i - 1].click()
                        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.TAG_NAME, "textarea")))

                    result_element.click()
                    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.TAG_NAME, "textarea")))

                judgment_url = driver.find_element('link text', 'PDF').get_attribute('href')
                result_data['judgment_url'] = judgment_url

                result_details.append(copy.deepcopy(result_data))
            except:
                print('Could not fetch a judgment url')

        result_details_df = pd.DataFrame.from_records(result_details)
        driver.close()
        result_details_df.drop_duplicates(subset=['judgment_date', 'petitioner_name', 'respondent_name'], inplace=True)
        result_details_df['judgment_id'] = result_details_df['judgment_url'].apply(lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest())
        return result_details_df

    def create_pdf_filepath_from_url(self, judgment_url: str, court='SC') -> str:
        ## TODO: check for the duplicate filenames
        file_name = judgment_url.split("/")[-1]
        if file_name[-4:] != ".pdf":
            alias = ''.join(random.choice(string.ascii_letters) for _ in range(30))
            file_name = alias + ".pdf"
        pdf_filepath = self.pdf_output_folder_path + court + "_" + file_name
        return pdf_filepath

    def create_txt_filepath_from_pdf_filepath(self, pdf_filepath: str, court='SC') -> str:
        ## TODO: check for the duplicate filenames
        file_name = os.path.splitext(os.path.basename(pdf_filepath))[0] + '.txt'
        txt_filepath = self.txt_output_folder_path + court + "_" + file_name
        return txt_filepath

    def download(self, judgment_url: str, pdf_filepath: str):
        try:
            if not os.path.isfile(pdf_filepath):
                # Download only if the file does not exist in output folder
                r = requests.get(judgment_url)
                temp_path = pdf_filepath + '_temp'
                open(temp_path, 'wb').write(r.content)

                # remove annotations and links including digital signatures from pdf file
                with open(temp_path, 'rb') as pdf_obj:
                    pdf = PyPDF2.PdfFileReader(pdf_obj)
                    out = PyPDF2.PdfFileWriter()

                    for page in pdf.pages:
                        out.addPage(page)
                    out.removeLinks()
                    with open(pdf_filepath, 'wb') as f:
                        out.write(f)

                os.remove(temp_path)

        except:
            pass

    def download_judgment_pdfs(self, search_results_metadata: pd.DataFrame) -> pd.DataFrame:
        """
        Function for downloading any file given its link
        Args:
        link (str): link to the file that is to be downloaded
        """
        os.makedirs(self.output_folder_path + '/pdfs', exist_ok=True)
        time.sleep(0.01)
        search_results_metadata['pdf_filepath'] = search_results_metadata['judgment_id'].apply(lambda x: os.path.join(self.pdf_output_folder_path , x +'.pdf'))
        judgment_urls = list(search_results_metadata[['judgment_url', 'pdf_filepath']].to_records(index=False))
        Parallel(n_jobs=-1)(
            delayed(self.download)(judgment_url, pdf_filepath) for judgment_url, pdf_filepath in tqdm(judgment_urls))
        return search_results_metadata

    def convert_downloaded_pdfs_to_text(self, search_results_metadata: pd.DataFrame):
        # search_results_metadata['txt_filepath'] = search_results_metadata['pdf_filepath'].apply(
        #     self.create_txt_filepath_from_pdf_filepath)
        search_results_metadata.apply(
            lambda x: read_one_pdf_file_convert_to_txt_and_write(self.converter, x['pdf_filepath'], self.txt_output_folder_path),axis=1)

    def search(self):
        if self.search_type=='free_text':
            df = self.search_free_text()

        elif self.search_type == 'actwise':
            df = self.search_actwise()
        else:
            df = pd.DataFrame()



        return df
if __name__ == '__main__':
    output_folder_path = '/Users/prathamesh/tw_projects/OpenNyAI/data/court_search/personal_liberty'
    s = SupremeCourtSearch(search_type='free_text', search_kw='personal liberty',
                           search_date_range=[datetime.date(2001, 1, 1), datetime.date(2022, 12, 31)],
                           output_folder_path=output_folder_path)

    # s = SupremeCourtSearch(search_type='actwise', search_kw='indian penal code',
    #                        search_date_range=[datetime.date(2022, 1, 1), datetime.date(2022, 12, 31)],
    #                        output_folder_path=output_folder_path)
    df = s.search()
    df.to_csv(os.path.join(output_folder_path,'search_results_judgment_metadata.csv'))
    df = s.download_judgment_pdfs(df)
    s.convert_downloaded_pdfs_to_text(df)

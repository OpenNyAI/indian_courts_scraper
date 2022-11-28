import os, re
from haystack.nodes.file_converter.pdf import PDFToTextConverter
from collections import Counter
from joblib import Parallel, delayed
from tqdm import tqdm

####### This notebook is to convert pdf to txt and save

max_lines_for_header_removal = 2
max_lines_for_footer_removal = 1
remove_header_footer_flag = True  ######## remove first line and footer with matching pattern

# Get the list of all files in directory tree at given path
listOfFiles = list()
full_path = list()


def remove_footer_by_pattern(pages):
    filtered_pages = []
    for page in pages:
        text_without_footer = re.sub(':::.*?\n', '', page)
        text_without_footer = re.sub('\s+\Z', '', text_without_footer)  ###### removing ending \n and spaces
        filtered_pages.append(text_without_footer)
    return filtered_pages


def split_last_few_sentences(text, sentence_cnt=1):
    split_text = re.split('\n+', text)
    all_sentence_cnt = len(split_text)

    seperators = re.findall('\n+', text)

    last_sentences = split_text[-sentence_cnt:]
    last_sentences_seperators = seperators[-sentence_cnt:]

    if all_sentence_cnt > sentence_cnt:
        remaining_text_sentences = split_text[0:all_sentence_cnt - sentence_cnt]
        remaining_text_seperators = seperators[0:all_sentence_cnt - sentence_cnt]

        last_sentences_seperators.append('')

        remanining_text_list = [sent + sep for sent, sep in zip(remaining_text_sentences, remaining_text_seperators)]
        remaining_text = ''.join(remanining_text_list)
    else:
        remaining_text = ''
        while len(last_sentences) < sentence_cnt:
            new_list = ['']
            new_list.extend(last_sentences)
            last_sentences = new_list
            last_sentences_seperators.append('')

    return remaining_text, last_sentences, last_sentences_seperators


def get_potential_sentences(page, to_remove):
    if to_remove == 'header':
        splitted_sentences = re.split('\n+', page, maxsplit=max_lines_for_header_removal)
        sentences_seperators = re.findall('\n+', page)[0:max_lines_for_header_removal]

        if len(splitted_sentences) > max_lines_for_header_removal:
            sentence_list = splitted_sentences[0:max_lines_for_header_removal]
            remaining_text = splitted_sentences[max_lines_for_header_removal]
        else:
            sentence_list = splitted_sentences[0:max_lines_for_header_removal]
            while (len(sentence_list) < max_lines_for_header_removal):
                sentence_list.append('')  ###### Add empty sentences
                sentences_seperators.append('')
            remaining_text = ''


    elif to_remove == 'footer':
        remaining_text, sentence_list, sentences_seperators = split_last_few_sentences(page,
                                                                                       sentence_cnt=max_lines_for_footer_removal)
    return (sentence_list, sentences_seperators, remaining_text)


def remove_digits_from_sentences(page_sentences_list):
    #### removes the digits from the sentences
    transformed_page_sentences_list = []
    for page_number, page_sentences in enumerate(page_sentences_list):
        transformed_page_sentences = []
        for sentence in page_sentences:
            if re.sub('\s', '', sentence) == str(page_number + 1):
                transformed_sentence = '__page_number__'
            else:
                transformed_sentence = re.sub('[0-9|\s]', '', sentence)
            transformed_page_sentences.append(transformed_sentence)
            # transformed_page_sentences = [re.sub('[0-9|\s]','',sentence) for sentence in page_sentences]
        transformed_page_sentences_list.append(transformed_page_sentences)
    return transformed_page_sentences_list


def repeating_pattern_check(sentences_list):
    repeating_pattern = None
    page_count = len(sentences_list)
    sentences_list = [x for x in sentences_list if not x == '']
    if len(sentences_list) == 0:
        return repeating_pattern

    ######### check if the potential footer/header is to be removed
    occurence_count = Counter(sentences_list)
    most_common_pattern, most_common_pattern_count = occurence_count.most_common(1)[0]
    # most_common_pattern_count = len([x for x in potential_haeder_footer_list if re.search(most_common_pattern, x)])
    if most_common_pattern_count > 0.7 * page_count:
        repeating_pattern = most_common_pattern

    return repeating_pattern


def get_repeating_pattern(transformed_page_sentences_list):
    ####### Returns list of repeating patterns
    repeating_pattern_list = []
    repeating_pattern_max_length = len(transformed_page_sentences_list[0])

    for i in range(repeating_pattern_max_length):
        sentences_at_given_position = [x[i] for x in transformed_page_sentences_list]
        repeating_pattern = repeating_pattern_check(sentences_at_given_position)
        if repeating_pattern is not None:
            repeating_pattern_list.append(repeating_pattern)
    return repeating_pattern_list


def get_removal_flag(page_sentences_list, repeating_patterns):
    ####### check if the repeating pattern is present in sentences. Return removal flags list
    removal_flag_list = []
    for page_sentences in page_sentences_list:
        removal_flags = [sentence in repeating_patterns for sentence in page_sentences]
        removal_flag_list.append(removal_flags)
    return removal_flag_list


def mark_consecutive_flags_true(removal_flags, to_remove):
    if to_remove == 'header':
        ###### mark previous flag of any True flag as True
        for sent_id, sentence_flags in enumerate(removal_flags):
            for idx, removal_flag in enumerate(sentence_flags):
                if idx > 0 and removal_flag:
                    removal_flags[sent_id][idx - 1] = True

    elif to_remove == 'footer':
        ###### mark previous flag of any True flag as True
        for sent_id, sentence_flags in enumerate(removal_flags):
            for idx, removal_flag in enumerate(sentence_flags):
                if idx <= len(sentence_flags) - 2 and removal_flag:
                    removal_flags[sent_id][idx + 1] = True
    return removal_flags


def check_for_repeating_pattern(page_sentence_list, to_remove):
    ###### returns list with removal flag for each sentence input list of lists
    transformed_page_sentence_list = remove_digits_from_sentences(page_sentence_list)
    repeating_patterns = get_repeating_pattern(transformed_page_sentence_list)
    removal_flags = get_removal_flag(transformed_page_sentence_list, repeating_patterns)
    removal_flags = mark_consecutive_flags_true(removal_flags, to_remove)
    return removal_flags


def remove_headers_or_footers(pages, to_remove='header'):
    page_potential_sentence_list = []
    potential_seperators_list = []
    remaining_text_list = []
    for page in pages:
        potential_sentence_list, potential_sentence_seperators, remaining_text = get_potential_sentences(page,
                                                                                                         to_remove)
        page_potential_sentence_list.append(potential_sentence_list)
        potential_seperators_list.append(potential_sentence_seperators)
        remaining_text_list.append(remaining_text)

    removal_list = check_for_repeating_pattern(page_potential_sentence_list, to_remove)

    pattern_removed_text_list = []
    for sentence_list, seperators, removal_flags, remaining_text in zip(page_potential_sentence_list,
                                                                        potential_seperators_list, removal_list,
                                                                        remaining_text_list):
        filtered_sent = [sent + sep for sent, sep, flag in zip(sentence_list, seperators, removal_flags) if not flag]
        pattern_removed_text = ''.join(filtered_sent)

        if to_remove == 'header':
            pattern_removed_text = pattern_removed_text + remaining_text
        elif to_remove == 'footer':
            pattern_removed_text = remaining_text + pattern_removed_text

        pattern_removed_text_list.append(pattern_removed_text)
    return pattern_removed_text_list


def clean_combine_pages(pages):
    cleaned_pages = remove_footer_by_pattern(pages)
    cleaned_pages = remove_headers_or_footers(cleaned_pages, to_remove='header')
    cleaned_pages = remove_headers_or_footers(cleaned_pages, to_remove='footer')

    cleaned_combined_text = "\n".join(cleaned_pages)
    return cleaned_combined_text


def read_all_pdf_files_from_directory_convert_to_txt_and_write(pdf_dir_path, txt_data_dir_path):
    listOfFiles = list()
    full_path = list()

    for (dirpath, dirnames, filenames) in os.walk(pdf_dir_path):
        listOfFiles += [file for file in filenames if bool(re.match(".*.pdf$", file))]
        full_path += [os.path.join(dirpath, file) for file in filenames if bool(re.match(".*.pdf$", file))]

    if not os.path.exists(txt_data_dir_path):
        os.makedirs(txt_data_dir_path)

    converter = PDFToTextConverter(remove_numeric_tables=True, valid_languages=["en"])

    Parallel(n_jobs=n_jobs)(
        delayed(read_one_pdf_file_convert_to_txt_and_write)(converter, path, txt_data_dir_path) for path in
        tqdm(full_path))

    # for path in full_path:
    #     read_one_pdf_file_convert_to_txt_and_write(converter, path, txt_data_dir_path)


def read_one_pdf_file_convert_to_txt_and_write(converter, pdf_file_path, txt_data_dir_path):
    try:
        pages = converter._read_pdf(file_path=pdf_file_path, layout=True)

        text = clean_combine_pages(pages)

        if not bool(re.match('^\n+$', text) or text == ''):
            ## write file only if there is some text
            case_name = os.path.splitext(os.path.basename(pdf_file_path))[0]
            txt_file_path = txt_data_dir_path + case_name + '.txt'
            f = open(txt_file_path, "w")
            f.write(text)
            f.close()
    except:
        print('could not read or convert file ' + pdf_file_path)






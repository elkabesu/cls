import MySQLdb
import re
import sys
import multiprocessing
import string
import nltk

from nltk.tag import pos_tag
from nltk.corpus import stopwords
from nltk.stem.porter import *
from nltk.tokenize import sent_tokenize, word_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from bs4 import BeautifulSoup
from collections import defaultdict

sys.setrecursionlimit(10000)

stopwords = stopwords.words('english')

cursor = mysql.cursor()
query = "select concat(\"/local/all_data/\", year, \"/\", substring_index(filename, \"/\", -1)) as path, dealid, purchaseprice from derek.20170408_temporary_mergermetrics_lite as a, edgar.edgar as b, derek.20170213_deals_formtype_breakdown as c where a.cik = b.cik and b.form_type = c.form_type and date_filed >= start and date_filed <= end limit 500"

cursor.execute(query)
deals = cursor.fetchall()
cursor.close()
mysql.close()

insert_query = "insert into 20170405_deals_prices_found_practice (path, event_num, price, code, date, bidders, score) values (\"%s\", \"%s\", %s, \"%s\", \"%s\", \"%s\", %s)"
bm_insert_query = "insert into 20170404_backgroundmerger_files (bm_path, event_num, hyperlink) values (\"%s\", \"%s\", \"%s\")"
"""
REGULAR EXPRESSIONS -- START
"""

background_regex1 = re.compile(
    ur'background[^<]{0,25}(merger|exchange|combination|offer|transaction)',
    re.IGNORECASE)
background_regex2 = re.compile(ur'background', re.IGNORECASE)
uppercase_regex = re.compile(ur'^[A-Z\s]{0,30}$', re.MULTILINE)

range_regex = re.compile(ur'\$(\d+\.\d+)[^<]{0,5}\$(\d+\.\d+)')
digitrange_regex = re.compile(ur'\$(\d+)[^<]{0,5}\$(\d+)')

digit_regex = re.compile('\d+\.\d+|\$|\%')
decimalwsign_regex = re.compile('\$\d+\.\d+')
digitwsign_regex = re.compile('\$\d+', re.IGNORECASE)
decimal_regex = re.compile('\d+\.\d+')
percent_regex = re.compile('\d+\.?\d*\%')

incorrect_regex = re.compile(ur'vot|salary|exercis|own|stock|invest',
                             re.IGNORECASE)

years = r'((?:19|20)\d\d)'
pattern = r'(%%s)\s*(%%s),\s*%s' % years
pattern_noyears = r'(%s)\s*(%s)'

thirties = pattern % ("September|April|June|November", r'0?[1-9]|[12]\d|30')

thirtyones = pattern % ("January|March|May|July|August|October|December",
                        r'0?[1-9]|[12]\d|3[01]')

thirties_noyears = pattern_noyears % ("February|September|April|June|November",
                                      r'0?[1-9]|[12]\d|30')

thirtyones_noyears = pattern_noyears % (
    "January|March|May|July|August|October|December", r'0?[1-9]|[12]\d|3[01]')

fours = '(?:%s)' % '|'.join('%02d' % x for x in range(4, 100, 4))

feb = r'(February) +(?:%s|%s)' % (
    r'(?:(0?[1-9]|1\d|2[0-8])), *%s' % years,  # 1-28 any year
    r'(?:(29), *((?:(?:19|20)%s)|2000))' % fours)  # 29 leap years only

result = '|'.join('(?:%s)' % x for x in (thirties, thirtyones, feb))
result_noyears = '|'.join('(?:%s)' % x
                          for x in (thirties_noyears, thirtyones_noyears))
r = re.compile(
    "%s|(January|March|May|July|August|October|December|September|April|June|November|February)\s*%s"
    % (result, years), re.IGNORECASE)
date_noyears = re.compile(result_noyears, re.IGNORECASE)
"""
REGULAR EXPRESSIONS -- END
"""

exclude = set(string.punctuation)

most_common_pp = open("20170328_most_common_phrases.txt", 'r').readlines()
most_common_pp = [pp.rstrip().lower() for pp in most_common_pp]
mcpp_dict = {}
for idx, m in enumerate(reversed(most_common_pp)):
    mcpp_dict[m] = idx + 1

most_common_phrases_in_background = [
    phrase.lstrip().rstrip()
    for phrase in open('most_common_phrases_in_background.txt', 'r').readlines(
    )
]
most_common_onebidder = [
    phrase.lstrip().rstrip()
    for phrase in reversed(
        open('most_common_onebidder.txt', 'r').readlines())
]
most_common_morebidders = [
    phrase.lstrip().rstrip()
    for phrase in reversed(
        open('most_common_morebidders.txt', 'r').readlines())
]


def stem_tokens(tokens, stemmer):
    stemmed = [stemmer.stem(item) for item in tokens]
    return stemmed


def get_filing_text(path):
    text = open(path, 'r').read()
    soup = BeautifulSoup(text)
    soup_str = str(soup)
    return soup, soup_str


def get_section_startend(item, bg_regex, soup_str, counter, section_start,
                         section_end, anybackground):
    if not item:
        return counter, section_start, section_end
    i_str = str(item)
    if anybackground:
        i_str_fixed = ' '.join(' '.join(
            BeautifulSoup(i_str).findAll(text=True)).split())
    else:
        i_str_fixed = ' '.join(i_str.split())
    if len(i_str) < 4:
        return counter, section_start, section_end

    if counter % 2 == 0:
        if bg_regex.search(i_str_fixed):
            if anybackground and len(i_str_fixed) > 20:
                return counter, section_start, section_end
            section_start = soup_str.index(i_str)
            counter += 1
    elif counter % 2 == 1:
        try:
            section_end = [
                m.start() for m in re.finditer(i_str, soup_str)
                if m.start() > section_start
            ][0]
        except:
            section_end = soup_str.find(i_str)
        section_diff = section_end - section_start
        if section_diff > 1000:
            counter += 1

    return counter, section_start, section_end


def get_backgroundmerger(soup, soup_str):
    section_start = section_end = counter = 0
    background = ''
    for i in soup.findAll('b'):
        counter, section_start, section_end = get_section_startend(
            i, background_regex1, soup_str, counter, section_start,
            section_end, False)

    if counter == 0:
        for i in soup.findAll(
                "font", style=re.compile(r'font-weight\s*:\s*bold',
                                         re.IGNORECASE)):
            counter, section_start, section_end = get_section_startend(
                i, background_regex1, soup_str, counter, section_start,
                section_end, False)

    if counter == 0:
        for i in soup.findAll('i'):
            counter, section_start, section_end = get_section_startend(
                i, background_regex1, soup_str, counter, section_start,
                section_end, False)

    if counter == 0:
        for i in soup.findAll('b'):
            counter, section_start, section_end = get_section_startend(
                i, background_regex2, soup_str, counter, section_start,
                section_end, True)

    if counter == 0:
        for i in soup.findAll('i'):
            counter, section_start, section_end = get_section_startend(
                i, background_regex2, soup_str, counter, section_start,
                section_end, True)

    if counter == 0:
        for i in re.findall(uppercase_regex, soup_str):
            try:
                counter, section_start, section_end = get_section_startend(
                    i, background_regex1, soup_str, counter, section_start,
                    section_end, False)
            except:
                continue

    background = ' '.join(
        BeautifulSoup(soup_str[section_start:section_end]).findAll(text=True))
    return background


def text_normalizer(background):
    text = ''.join(ch for ch in background.lower() if ch not in exclude)

    tokens = word_tokenize(text)
    filtered = [w for w in tokens if not w in stopwords]

    stemmer = PorterStemmer()
    stemmed = stem_tokens(filtered, stemmer)
    normalized_text = ' '.join(stemmed)
    return normalized_text


def get_bidders_score(most_common_list, normalized_text):
    scorer = sum([
        score for score, b in enumerate(most_common_list)
        if b in normalized_text
    ])
    return scorer


def get_bidder(normalized_text):
    countone = countmore = 0
    for ph in most_common_phrases_in_background:
        if ph in normalized_text:
            normalized_text = normalized_text.replace(ph, ' ')
            normalized_text = ' '.join(normalized_text.split())

    count1 = get_bidders_score(most_common_onebidder, normalized_text)
    countmore = get_bidders_score(most_common_morebidders, normalized_text)

    if countmore == 0:
        countmore = 1

    if countone / countmore > .65:
        extracted_bid = 'one'
    else:
        extracted_bid = 'more'


def get_current_year(text):
    try:
        most_current_year = re.search(years, text).group()
        return most_current_year
    except:
        return


def combine_date(date_list, missing_year):
    if missing_year:
        return [
            "%s %s" % (' '.join(d).lstrip().rstrip().lower(), missing_year)
            for d in date_list
        ]
    else:
        return [' '.join(d).lstrip().rstrip().lower() for d in date_list]


def get_correctdate(date, date_noyears, paragraphdate, most_current_year):
    if date:
        correctdate = combine_date(date, False)
    elif date_noyears:
        correctdate = combine_date(date_noyears, most_current_year)
    elif paragraphdate:
        correctdate = combine_date(paragraphdate, False)

    try:
        return correctdate
    except:
        return ['unknown date']


def insert_offer_into_database(path, event_num, price, code, date,
                               extracted_bid, phrase_count):
    cursor = mysql.cursor()
    cursor.execute(insert_query % (path, event_num, price, code, date,
                                   extracted_bid, phrase_count))
    mysql.commit()
    cursor.close()
    mysql.close()


def get_average_range_price(range_grabbed):
    float1 = float(range_grabbed[0][0].replace('$', '').lstrip().rstrip())
    float2 = float(range_grabbed[0][1].replace('$', '').lstrip().rstrip())
    average_range_price = (float1 + float2) / 2
    return average_range_price


def deny_closingprice_and_millionbillion(sentence, idx_of_d):
    clos_in_sentence = [m.start() for m in re.finditer('clos', sentence)]
    illion_in_sentence = [m.start() for m in re.finditer('illion', sentence)]
    for cis in clos_in_sentence:
        if idx_of_d - cis < 70 and idx_of_d - cis > -30:
            return True
    for iis in illion_in_sentence:
        if abs(idx_of_d - iis) < 10:
            return True
    return False


def get_current_price(d, purchase_price_float, check_purchase_price):
    current_price_float = float(
        d.replace('$', '').replace('s', '').replace('%', '').lstrip().rstrip())
    if check_purchase_price:
        if abs(purchase_price_float -
               current_price_float) / current_price_float > .8:
            return False
    else:
        return current_price_float


def extract_offer_signed(regex, sentence, correctdate, path, event_num,
                         extracted_bid, phrase_count, extracted_list,
                         purchase_price_float):
    range_grabbed = re.findall(regex, sentence)
    if range_grabbed:
        average_range_price = get_average_range_price(range_grabbed)
        for cd in correctdate:
            insert_offer_into_database(path, event_num, average_range_price,
                                       'c', cd, extracted_bid, phrase_count)
        return
    for d in extracted_list:
        idx_of_d = sentence.index(d)
        if deny_closingprice_and_millionbillion(sentence, idx_of_d):
            continue
        current_price_float = get_current_price(d, purchase_price_float, True)
        if not current_price_float:
            continue
        for cd in correctdate:
            insert_offer_into_database(path, event_num, current_price_float,
                                       'c', cd, extracted_bid, phrase_count)
    return


def extractor((deal)):
    #for deal in deals:
    try:
        path = deal[0]
        event_num = deal[1]

        soup, soup_str = get_filing_text(path)
        background = get_backgroundmerger(soup, soup_str)

        if background:
            normalized_text = text_normalizer(background)
            extracted_bid = get_bidder(normalized_text)
        else:
            return

        background_paragraphs = re.split('\s{4,}', background)
        purchase_price_float = float(deal[2].replace('$', ''))
        most_current_year = grabbed_paragraphdate = ''
        for background_paragraph in background_paragraphs:

            price = date = ''
            paragraph = ' '.join(background_paragraph.split()).lower()
            sentences = sent_tokenize(paragraph)
            most_current_year = get_current_year(paragraph)

            temp_gpd = re.findall(r, paragraph)
            if temp_gpd:
                grabbed_paragraphdate = temp_gpd

            for sentence in sentences:
                phrase_count = 0
                most_current_year = get_current_year(sentence)
                for pp in mcpp_dict:
                    if pp in sentence:
                        phrase_count += mcpp_dict[pp]

                if digit_regex.search(sentence) and phrase_count > 10:
                    grabbed_date = re.findall(r, sentence)
                    grabbed_date_noyears = re.findall(date_noyears, sentence)
                    correctdate = get_correctdate(
                        grabbed_date, grabbed_date_noyears,
                        grabbed_paragraphdate, most_current_year)

                    decimalwsign = re.findall(decimalwsign_regex, sentence)
                    digitwsign = re.findall(digitwsign_regex, sentence)
                    decimal = re.findall(decimal_regex, sentence)
                    percent = re.findall(percent_regex, sentence)
                    if decimalwsign:
                        extract_offer_signed(
                            range_regex, sentence, correctdate, path,
                            event_num, extracted_bid, phrase_count,
                            decimalwsign, purchase_price_float)

                    elif digitwsign:
                        extract_offer_signed(digitrange_regex, sentence,
                                             correctdate, path, event_num,
                                             extracted_bid, phrase_count,
                                             digitwsign, purchase_price_float)

                    elif decimal:
                        for d in decimal:
                            current_float = get_current_price(
                                d, purchase_price_float, False)
                            if current_float > 0 and current_float < 3:
                                for cd in correctdate:
                                    insert_offer_into_database(
                                        path, event_num, current_float, 'p',
                                        cd, extracted_bid, phrase_count)
                    elif percent:
                        if incorrect_regex.search(
                                sentence) or phrase_count < 300:
                            continue
                        for p in percent:
                            current_percent = get_current_price(
                                p, purchase_price_float, False)
                            pasd = current_percent / 100
                            for cd in correctdate:
                                insert_offer_into_database(
                                    path, event_num, pasd, 'p', cd,
                                    extracted_bid, phrase_count)

    except Exception, e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print str(e), exc_tb.tb_lineno
        return


def mp_handler():
    p = multiprocessing.Pool(22)
    p.map(extractor, deals)
    p.close()
    p.join()


if __name__ == '__main__':
    mp_handler()


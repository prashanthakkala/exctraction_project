import os
import torch
import re
import copy
import json
import time
import pandas as pd
import sys
import pdfplumber
import numpy as np
from difflib import SequenceMatcher as SM
from io import BytesIO
# from pyxlsb import open_workbook as open_xlsb
import urllib.request
import warnings
from functools import reduce
warnings.filterwarnings('ignore')
# reference_name=fpath
import fitz


def main(fpath,cpath):
    try:
        # global upload_file
        sys.path.append(os.getcwd())
        ######################################
        # Form
        # sub-keys checking
        # import copy
        # import os
        # import pandas as pd
        # import pdfplumber
        # import numpy as np
        # from difflib import SequenceMatcher as SM

        master_data_dict = {'Values': ['11', '11A', '11B', '11C', '11D', '11E', '11F', '11G', '11H', '11I', '13', '13A', '13B', '13C', '13D', '13E', '13F', '13G', '13H', '13I', '13J', '13K', '13L', '13M', '13N', '13O', '13P', '13Q', '13R', '13S', '13T', '13U', '13V', '13W', '13X', '14', '14A', '14B', '14C', '15', '15A', '15B', '15C', '15D', '15E', '15F', '15G', '15H', '15I', '15J', '15K', '15L', '15M', '15N', '15O', '15P', '16', '16A', '16B', '16C', '16E', '16F', '16G', '16H', '16I', '16J', '16L', '16M', '16N', '16O', '16P', '16Q', '16R', '16S', '16T', '16W', '16X', '17', '17A', '17B', '17C', '17D', '17E', '17F', '18', '18A', '18B', '18C', '19', '19A', '19B', '19C', '20', '20A', '20B', '20C', '20D', '20E', '20F', '20G', '20H', '20I', '20J', '20K', '20L', '20M', '20N', '20O', '20P', '20Q', '20R', ' 20S', '20T', '20U', '20V', '20W', '20X', '20Y', '20Z', '20AA', '20AB', '20AC', '20AD', '20AE', '20AF', '20AG', '20AH'],
            'Header': ['OTHER INCOME (LOSS)', 'OTHER PORTFOLIO INCOME (LOSS)', 'INVOLUNTARY CONVERSIONS', 'SEC. 1256 CONTRACTS & STRADDLES', 'MINING EXPLORATION COSTS RECAPTURE', 'CANCELLATION OF DEBT', 'SECTION 743(B) POSITIVE ADJUSTMENTS', 'SECTION 965(A) INCLUSION', 'SUBPART F INCOME OTHER THAN SECTIONS 951A AND 965 \nINCLUSION', 'OTHER INCOME (LOSS)', 'OTHER DEDUCTIONS', 'CASH CONTRIBUTIONS (60%)', 'CASH CONTRIBUTIONS (30%)', 'NONCASH CONTRIBUTIONS (50%)', 'NONCASH CONTRIBUTIONS (30%)', 'CAPITAL GAIN PROPERTY TO A 50% ORGANIZATION (30%)', 'CAPITAL GAIN PROPERTY (20%)', 'CONTRIBUTIONS (100%)', 'INVESTMENT INTEREST EXPENSE', 'DEDUCTIONS-ROYALTY INCOME', 'SECTION 59(E)(2) EXPENDITURES', 'EXCESS BUSINESS INTEREST EXPENSE', 'DEDUCTIONS - PORTFOLIO (OTHER)', 'AMOUNTS PAID FOR MEDICAL INSURANCE', 'EDUCATIONAL ASSISTANCE BENEFITS', 'DEPENDENT CARE BENEFITS', 'PREPRODUCTIVE PERIOD EXPENSES', 'COMMERCIAL REVITALIZATION DEDUCTION FROM RENTAL REAL ESTATE \nACTIVITIES', 'PENSIONS AND IRAS', 'REFORESTATION EXPENSE DEDUCTION', 'THROUGH U. RESERVED FOR FUTURE USE', 'RESERVED FOR FUTURE USE', 'SECTION 743(B) NEGATIVE ADJUSTMENTS', 'OTHER DEDUCTIONS', 'SECTION 965(C) DEDUCTION', 'SELF-EMPLOYMENT EARNINGS (LOSS)', 'NET EARNINGS (LOSS) FROM SELF-EMPLOYMENT', 'GROSS FARMING OR FISHING INCOME', 'GROSS NON-FARM INCOME', 'CREDITS', 'RESERVED FOR FUTURE USE', 'LOW-INCOME HOUSING CREDIT (OTHER) FROM PRE-2008 BUILDINGS', 'LOW-INCOME HOUSING CREDIT (SECTION 42(J)(5)) FROM POST-2007 BUILDINGS', 'LOW-INCOME HOUSING CREDIT (OTHER) FROM POST-2007 BUILDINGS', 'QUALIFIED REHABILITATION EXPENDITURES (RENTAL REAL ESTATE)', 'OTHER RENTAL REAL ESTATE CREDITS', 'OTHER RENTAL CREDITS', 'UNDISTRIBUTED CAPITAL GAINS CREDIT', 'BIOFUEL PRODUCER CREDIT', 'WORK OPPORTUNITY CREDIT', 'DISABLED ACCESS CREDIT', 'EMPOWERMENT ZONE EMPLOYMENT CREDIT', 'CREDIT FOR INCREASING RESEARCH ACTIVITIES', 'CREDIT FOR EMPLOYER SOCIAL SECURITY AND MEDICARE TAXES', 'BACKUP WITHHOLDING', 'OTHER CREDITS', 'FOREIGN TRANSACTIONS', 'NAME OF COUNTRY OR U.S. POSSESSION', 'GROSS INCOME FROM ALL SOURCES', 'GROSS INCOME SOURCED AT PARTNER LEVEL', 'FOREIGN GROSS INCOME SOURCED AT PARTNERSHIP LEVEL - \nFOREIGN BRANCH CATEGORY', 'FOREIGN GROSS INCOME SOURCED AT PARTNERSHIP LEVEL - \nPASSIVE CATEGORY', 'FOREIGN GROSS INCOME SOURCED AT PARTNERSHIP LEVEL - \nGENERAL CATEGORY', 'FOREIGN GROSS INCOME SOURCED AT PARTNERSHIP LEVEL - \nOTHER', 'DEDUCTIONS ALLOCATED AND APPORTIONED AT PARTNER LEVEL - \nINTEREST EXPENSE', 'DEDUCTIONS ALLOCATED AND APPORTIONED AT PARTNER LEVEL - \nOTHER', 'DEDUCTIONS ALLOCATED AND APPORTIONED AT PARTNERSHIP \nLEVEL TO FOREIGN SOURCE INCOME - FOREIGN BRANCH CATEGORY', 'DEDUCTIONS ALLOCATED AND APPORTIONED AT PARTNERSHIP \nLEVEL TO FOREIGN SOURCE INCOME - PASSIVE CATEGORY', 'DEDUCTIONS ALLOCATED AND APPORTIONED AT PARTNERSHIP \nLEVEL TO FOREIGN SOURCE INCOME - GENERAL CATEGORY', 'DEDUCTIONS ALLOCATED AND APPORTIONED AT PARTNERSHIP \nLEVEL TO FOREIGN SOURCE INCOME - OTHER', 'OTHER INFORMATION - TOTAL FOREIGN TAXES PAID', 'OTHER INFORMATION - TOTAL FOREIGN TAXES ACCRUED', 'OTHER INFORMATION - REDUCTION IN TAXES AVAILABLE FOR \nCREDIT', 'OTHER INFORMATION - FOREIGN TRADING GROSS RECEIPTS', 'OTHER INFORMATION - EXTRATERRITORIAL INCOME EXCLUSION', 'OTHER INFORMATION - SECTION 965A INFORMATION', 'OTHER INFORMATION - OTHER FOREIGN TRANSACTION', 'ALTERNATIVE MINIMUM TAX (AMT) ITEMS', 'POST-1986 DEPRECIATION ADJUSTMENT', 'ADJUSTED GAIN OR LOSS', 'DEPLETION (OTHER THAN OIL & GAS)', 'OIL, GAS, & GEOTHERMAL-GROSS INCOME', 'OIL, GAS, & GEOTHERMAL-DEDUCTIONS', 'OTHER AMT ITEMS', 'TAX-EXEMPT INCOME AND NONDEDUCTIBLE EXPENSES', 'TAX-EXEMPT INTEREST INCOME', 'OTHER TAX-EXEMPT INCOME', 'NONDEDUCTIBLE EXPENSES', 'DISTRIBUTIONS', 'CASH AND MARKETABLE SECURITIES', 'DISTRIBUTION SUBJECT TO SECTION 737', 'OTHER PROPERTY', ' OTHER INFORMATION', ' INVESTMENT INCOME', 'INVESTMENT EXPENSES', ' FUEL TAX CREDIT INFORMATION', 'QUALIFIED REHABILITATION EXPENDITURES (OTHER THAN RENTAL \nREAL ESTATE)', 'BASIS OF ENERGY PROPERTY', 'RECAPTURE OF LOW-INCOME HOUSING CREDIT (SECTION 42(J)(5))', 'RECAPTURE OF LOW-INCOME HOUSING CREDIT (OTHER)', 'RECAPTURE OF INVESTMENT CREDIT', 'RECAPTURE OF OTHER CREDITS', 'LOOK-BACK INTEREST-COMPLETED LONG-TERM CONTRACTS', 'LOOK-BACK INTEREST-INCOME FORECAST METHOD', 'DISPOSITIONS OF PROPERTY WITH SECTION 179 DEDUCTIONS', 'RECAPTURE OF SECTION 179 DEDUCTION', 'INTEREST EXPENSE FOR CORPORATE PARTNERS', 'SECTION 453(L)(3) INFORMATION', 'SECTION 453A(C) INFORMATION', 'SECTION 1260(B) INFORMATION', 'INTEREST ALLOCABLE TO PRODUCTION EXPENDITURES', 'CCF NONQUALIFIED WITHDRAWALS', 'DEPLETION INFORMATION - OIL AND GAS', 'RESERVED', 'UNRELATED BUSINESS TAXABLE INCOME', 'PRECONTRIBUTION GAIN (LOSS)', 'SECTION 108(I) INFORMATION', 'NET INVESTMENT INCOME', 'SECTION 199A INFORMATION', 'SECTION 704(C) INFORMATION', 'SECTION 751 GAIN (LOSS)', 'SECTION 1(H)(5) GAIN (LOSS)', 'DEEMED SECTION 1250 UNRECAPTURED GAIN', 'EXCESS TAXABLE INCOME', 'EXCESS BUSINESS INTEREST INCOME', 'GROSS RECEIPTS FOR SECTION 59A(E)', 'OTHER INFORMATION']}
        master_data = pd.DataFrame(master_data_dict)
        # master_data.to_csv('new_master_data.csv')

        # Model
        def checkbox(pdf_path):
            '''This function takes pdf as input and returns check box pattern corresponding value as output'''
            # initalizing model
            # model = torch.hub.load('ultralytics/yolov5', 'custom',path="app/model.pt",force_reload=True)
            model = torch.hub.load('ultralytics/yolov5', 'custom',path="model.pt",force_reload=True)            
            # converting pdf to images corresponding to number of pdf pages
            # outputpath = "pdf"
            # result = pdf2jpg.convert_pdf2jpg(pdf_path, outputpath ,pages="ALL")
            zoom_x = 2.0  # horizontal zoom
            zoom_y = 2.0  # vertical zoom
            mat = fitz.Matrix(zoom_x, zoom_y)
            #patht="/code/./coreservice/pdf"
            print(os.getcwd())
            print(os.listdir())
            doc = fitz.open(pdf_path)
            #os.chdir(patht)
            image_pages=[]
            for page in doc:  # iterate through the pages
                pix = page.get_pixmap(matrix=mat)  # render page to an image
                pix.save("%i.png" % page.number)
                photo="%i.png" % page.number
                image_pages.append(photo)
                print("%i.png" % page.number)
            #image_pages = os.listdir(patht)
            print(image_pages)
            
            final_result = [] # dictionary of page and its corresponding extracted values
            for sort_idx in range(len(image_pages)):
                sorted_page = list(filter(lambda x: str(sort_idx) == x.split('.')[0], image_pages))[0]
                print('sorted_page value is:', sorted_page)
                results = model(sorted_page)
                os.remove(sorted_page)
                df=results.pandas().xyxy[0]
                data_s=df["name"].tolist()
                data=list(set(data_s))
                if len(data) > 0:
                    data = [i.replace('Checked', '').strip() for i in data if i.split()[0] in ['Checked', 'Yes', 'No']]
                final_result.append(data)
            print(final_result)
            # dir = f"pdf/{pdf_path}_dir"
            # for f in os.listdir(dir):
            #     os.remove(os.path.join(dir, f))
            # print(os.getcwd())
            # print(os.listdir())
            # os.chdir("../..")
            # print(os.getcwd())
            # print(os.listdir())
            return final_result 


        def horizontal_merge(wrd_lst, rng = 12):
            wrd_hz = []
            test = copy.deepcopy(wrd_lst)
            for wrd in test:
                if len(wrd['text'].strip()) == 0 or (wrd['top'] - wrd['bottom'] == 0):
                    continue
                if len(wrd_hz) == 0:
                    wrd_hz.append(wrd)
                else:
                    diff_val = abs(wrd['x0']- wrd_hz[len(wrd_hz) - 1]['x1'])
                    if len(wrd['text'].strip()) == 0:
                        continue
                    elif 0 <= diff_val < rng:
                        if diff_val < 2:
                            wrd['text'] = wrd_hz[len(wrd_hz) - 1]['text']+wrd['text']
                        else:
                            wrd['text'] = wrd_hz[len(wrd_hz) - 1]['text']+' '+wrd['text']
                        wrd['x0'] = min([wrd_hz[len(wrd_hz) - 1]['x0'], wrd['x0']])
                        wrd['x1'] = max([wrd_hz[len(wrd_hz) - 1]['x1'], wrd['x1']])
                        wrd['top'] = min([wrd_hz[len(wrd_hz) - 1]['top'], wrd['top']])
                        wrd['doctop'] = min([wrd_hz[len(wrd_hz) - 1]['doctop'], wrd['doctop']])
                        wrd['bottom'] = max([wrd_hz[len(wrd_hz) - 1]['bottom'], wrd['bottom']])
                        wrd_hz.pop(-1)
                        wrd_hz.append(wrd)
                    else:
                        wrd_hz.append(wrd)
            return wrd_hz

        def text_cleaner(pdf_words_):
            pdf_words = copy.deepcopy(pdf_words_)
            result = []
            unclean =[]
            for wrds in pdf_words:
                wrds_lp = []
                for wrd in wrds:
                    if ('}}}' not in wrd['text']) and \
            ('~~~' not in wrd['text']) and ('---' not in wrd['text']) \
            and ('===' not in wrd['text']):
                        wrds_lp.append(wrd)
                    else:
                        unclean.append(wrd)
                result.append(wrds_lp)
            return result

        def joiner(wrd):
            '''This function takes a string word as input and it joins the
            given string by removing spaces beween them and replacing "." with empty value and 
            returns the lower case of the joined wrd. It is mainly used for comparing and searching purpose 
            in the code.'''
            result = ''.join([i for i in wrd.split()]).replace('.','').strip().lower()
            return result

        def word_match(words, pdf_words):
            if '\n' in words:
                words_ = words.split('\n')
                match_found_ = []
                for word in words_:
                    match = list(filter(lambda x: joiner(word) in joiner(x['text']), pdf_words))
                    if len(match) == 0:
                        return []
                    else:
                        match_found_.append(match[0])
                final_word = []
                match_found = copy.deepcopy(match_found_)
                for match_val in match_found:
                    if len(final_word) == 0:
                        final_word.append(match_val)
                    else:
                        diff_val = final_word[len(final_word)-1]['bottom'] - match_val['top']
                        if diff_val < 3:
                            match_val['text'] = final_word[len(final_word)-1]['text'] + ' '+ match_val['text']
                            match_val['x0'] = min([final_word[len(final_word)-1]['x0'] , match_val['x0']])
                            match_val['top'] = min([final_word[len(final_word)-1]['top'] , match_val['top']])
                            match_val['doctop'] = min([final_word[len(final_word)-1]['doctop'] , match_val['doctop']])
                            match_val['x1'] = max([final_word[len(final_word)-1]['x1'] , match_val['x1']])
                            match_val['bottom'] = max([final_word[len(final_word)-1]['bottom'] , match_val['bottom']])
                            final_word.pop(-1)
                            final_word.append(match_val)
                        else:
                            break
            else:
                final_word = list(filter(lambda x: joiner(words) in joiner(x['text']), pdf_words))
                if len(final_word) == 0:
                    return []
            return final_word

        def value_extractor(pdf_pages, header, stp, form):   
        #     print('header value is:', header)
            try:
                pdf_words_check = [horizontal_part12(wrds) for wrds in pdf_words_init]
                pdf_words_ = [horizontal_merge(wrds) for wrds in pdf_words_init]
                pdf_words_clean = text_cleaner(pdf_words_)
                pdf_words_clean = [mean_creator(i) for i in pdf_words_clean]

                pg_cnt = -1
                for pdf_words in pdf_words_clean[:stp]:
                    pg_cnt += 1
                    header_mtch = word_match(header, pdf_words)
                    header_splt = header.split(',')

                    if len(header_splt) == 3:
                        line_header = header_splt[-2].replace('BOX', 'LINE').strip()+' - '+header_splt[0]
            #                 print('\n line_header value is:', line_header, '\n')
                        header_mtch_2 = word_match(line_header, pdf_words)
            #                 print('header_mtch_2 value is:', header_mtch_2)
                    else:
                        header_mtch_2 = []

                    if len(header_mtch) > 0:
                        break
                    elif len(header_mtch_2) > 0:
                        header_mtch = header_mtch_2
                        break
                    else:
                        header_last_mtch = word_match(header_splt[-1], pdf_words)

                        if len(header_last_mtch) > 0:
                            header_mtch = list(filter(lambda x: joiner(header_splt[-2]) in joiner(x['text']), header_last_mtch))

                            if len(header_mtch) > 0:

                                header_mtch = list(filter(lambda x: joiner(header_splt[0].strip().split(' ')[0]) in joiner(x['text']), header_mtch))

                            if len(header_mtch) > 0:
                                break
                        else:
                            if len(header_splt) == 3:
                                splt_1 = header_splt[1].replace('BOX', 'LINE')
                                splt_2 = header_splt[2].replace('CODE', '').strip()
                                splt_3 = header_splt[0].strip()
                                line_header_new = splt_1+' ' +splt_2+ ' '+splt_3
            #                         print('line_header_new value is:', line_header_new)
                                header_splt_1_check = list(filter(lambda x: joiner(splt_1) in joiner(x['text']), pdf_words))
            #                         print('splt_1_idx match value is:', header_splt_1_check)
                                if len(header_splt_1_check) > 0:
                                    splt_1_idx = pdf_words.index(header_splt_1_check[0])
                                    nxt_two_wrds = ' '.join([i['text'] for i in pdf_words[splt_1_idx:splt_1_idx+3]])
            #                             print('nxt_two_wrds value is:', nxt_two_wrds)
                                    if joiner(line_header_new) in joiner(nxt_two_wrds):
                                        header_mtch = header_splt_1_check
                                        break                    
        #         print('header_mtch value is:', header_mtch,'\n')
                if len(header_mtch) > 0:
            #             print('header_mtch value is:', header_mtch,'\n')

                    pdf_words_new = [wrd for wrd in pdf_words_clean[pg_cnt] if wrd['top'] > header_mtch[0]['bottom']]
                    if len(pdf_words_new) > 0:
                        prv_wrd = []
                        first_match = pdf_words_new[0]
                        first_match_words = copy.deepcopy(pdf_words_new)
                        first_match_words = [i for i in first_match_words if i['x0'] < first_match['x1']]
                        for wrd in first_match_words:
                            if len(prv_wrd) == 0:
                                prv_wrd.append(wrd)
                            else:
                                diff_val = wrd['top'] - prv_wrd[len(prv_wrd) -1]['bottom']
                                if diff_val > 23:
                                    break

                                else:
                                    prv_wrd.append(wrd)
                        ending_key = prv_wrd[-1:]

                        ending_column = [i for i in pdf_words_new if i['bottom'] >= first_match['top']
                                        and i['top'] <= first_match['bottom']-1 and i['x0'] > first_match['x1']]

                        if len(ending_column) > 1:

                            column_cnt = 0
                            ending_column = ending_column
                        elif len(ending_column) == 1:
                            column_cnt = 1
                            new_value = []
                            if '    ' in ending_column[0]['text']:
                                split_chk = ending_column[0]['text'].split()

                                if 0 < len(split_chk) < 3:
                                    column_cnt = 0
                                    column_copy = copy.deepcopy(ending_column)

                                    for idx in range(len(split_chk)):
                                        column_copy = copy.deepcopy(ending_column)
                                        column_copy = column_copy[0]
                                        column_copy['text'] = split_chk[idx]
                                        if idx == 0:
                                            column_copy['x1'] = (column_copy['x0'] + column_copy['x1'])/2
                                        else:
                                            column_copy['x0'] = ((column_copy['x0'] + column_copy['x1'])/2) + 3
                                        new_value.append(column_copy)
                                    ending_column = new_value
                                else:
                                    ending_column = ending_column
                            else:
                                ending_column = ending_column
                        else:
                            column_cnt = 1
                            first_match_ = copy.deepcopy(first_match)
                            first_match_['text'] = ''
                            first_match_['x0'] += 50
                            ending_column = [first_match_]

                    first_match_words_copy = copy.deepcopy(first_match_words)
                    first_match_words_copy = [i for i in first_match_words_copy if i['top'] < ending_key[0]['bottom']]
                    no_val = []
                    final_val = []
                    for wrd in first_match_words_copy:
                        value_chk = [i for i in pdf_words_new if i['top'] >= wrd['top']-1
                                    and i['bottom'] <= wrd['bottom'] and i['x0'] > wrd['x1'] and
                                    i['x1'] > ending_column[0]['x0'] and i['top'] < ending_key[0]['bottom']]

                        value_num_chk_ = []
                        value_num_chk = ''.join([i for wr_ in value_chk[:] for i in wr_['text'] if i.isnumeric()])
                        if len(value_num_chk) > 2:
                            if len(no_val) > 0:
                                for loop_idx in range(len(no_val)):
                                    diff_val = wrd['top'] - no_val[-1]['bottom']
                                    if diff_val < 2.5:
                                        wrd['text'] = no_val[-1]['text'] + ' ' + wrd['text']
                                        wrd['x0'] = min([wrd['x0'], no_val[-1]['x0']])
                                        wrd['x1'] = max([wrd['x1'], no_val[-1]['x1']])
                                        wrd['top'] = min([wrd['top'], no_val[-1]['top']])
                                        wrd['doctop'] = min([wrd['doctop'], no_val[-1]['doctop']])
                                        wrd['bottom'] = max([wrd['bottom'], no_val[-1]['bottom']])
                                        no_val.pop(-1)
                                    else:
            #                             print('\nloop ckt no_val is:', no_val)
                                        [final_val.append(i) for i in vertical_over_flow(no_val, space_ = ' \n')]
                                        no_val = []
                                        break
                                final_val.append(wrd)
                            else:
                                final_val.append(wrd)

                        else:
                            no_val.append(wrd)
            #         print('\nouter no_val is:', no_val)
                    if len(no_val) > 0:
                        [final_val.append(i) for i in vertical_over_flow(no_val, space_ = ' \n')]
                    result = {}
                    # columns
                    result[first_match['text']] = []
                    for idx in range(len(ending_column)):
                        result[ending_column[idx]['text']] = []
                    # column values
                    for key_vlu in final_val:
                        result[first_match['text']].append(key_vlu['text'])
                        pdf_words_new_2 = [i for i in pdf_words_new if i['x0'] > first_match['x0']]
                        pdf_words_new_1 = [wrd for wrd in horizontal_part12(pdf_words_init[pg_cnt], rng =0.01) if wrd['top'] > header_mtch[0]['bottom']
                                        and wrd['x0'] > first_match['x0']]

                        for idx in range(len(ending_column)):

                            if column_cnt == 0:
                                final_match = [i for i in pdf_words_new_2 if i['x0'] < ending_column[idx]['x1'] +25 and i['x1'] > ending_column[idx]['x0']
                                            and i['top'] > key_vlu['top']-1 and i['bottom'] < key_vlu['bottom']+1]
                            else:
            #                         print('final_match else:condition', header_mtch)
                                final_match = [i for i in pdf_words_new_1 if i['x1'] > ending_column[idx]['x0']
                                            and i['top'] >= key_vlu['top']-1 and i['y_mean'] <= key_vlu['bottom']+1]
                                if len(final_match) == 0:
                                    final_match = [i for i in pdf_words_new_2 if i['x1'] > ending_column[idx]['x0']
                                            and i['top'] >= key_vlu['top']-1 and i['y_mean'] <= key_vlu['bottom']+1]

                            if len(final_match) > 0:
                                result[ending_column[idx]['text']].append(final_match[-1]['text'])
                            else:
                                result[ending_column[idx]['text']].append('')
                    result_df= pd.DataFrame(result)
                    row2 = pd.Series(result_df.columns, index=result_df.columns)
                    columns = []
                    for idx in range(len(result_df.columns)):
                        if idx == 0:
                            columns.append('Key')
                        elif idx == 1:
                            columns.append('Values')
                        else:
                            columns.append('')
                    row1 = [' ' for idx in range(len(result_df.columns))]
                    row1[0] = f'{header_mtch[0]["text"]} ---> PageNo: {pg_cnt+1}'
                    result_df.loc[-2] = row1
                    result_df.loc[-1] = row2
                    result_df.index = result_df.index + 2
                    result_df.sort_index(inplace = True)
                    result_df.columns = columns
                    result_df.drop_duplicates(inplace = True)
                    result_df.reset_index(inplace = True, drop = True)

                    return result_df
                else:
                    return pd.DataFrame(['Please check the details'], columns=['Warning'])
            except Exception as err:
                return pd.DataFrame([err], columns=['Error'])
        def less_horizontal_merge(wrd_lst, num_lst):
            wrd_hz = []
            test = copy.deepcopy(wrd_lst)
            for wrd in test:
                wrd['text'].replace('▶','').strip()
                if len(wrd['text'].strip()) == 0:
                    continue
                if len(wrd_hz) == 0:
                    wrd_hz.append(wrd)
                else:
                    diff_val = abs(wrd['x0']- wrd_hz[len(wrd_hz) - 1]['x1'])
                    diff_vert = abs(wrd['bottom']- wrd_hz[len(wrd_hz) - 1]['bottom'])
                    if len(wrd['text'].strip()) == 0:
                        continue
                    elif 0 <= diff_val < 6.3 and diff_vert < 3:
                        if wrd_hz[len(wrd_hz) - 1]['text'].strip() in num_lst and diff_val > 1 or wrd_hz[len(wrd_hz) - 1]['text'][-1] == '.':
                            wrd_hz.append(wrd)
                        else:
                            if diff_val < 1.5:
                                wrd['text'] = wrd_hz[len(wrd_hz) - 1]['text']+wrd['text']
                            else:
                                wrd['text'] = wrd_hz[len(wrd_hz) - 1]['text']+' '+wrd['text']
                            wrd['x0'] = min([wrd_hz[len(wrd_hz) - 1]['x0'], wrd['x0']])
                            wrd['x1'] = max([wrd_hz[len(wrd_hz) - 1]['x1'], wrd['x1']])
                            wrd['top'] = min([wrd_hz[len(wrd_hz) - 1]['top'], wrd['top']])
                            wrd['doctop'] = min([wrd_hz[len(wrd_hz) - 1]['doctop'], wrd['doctop']])
                            wrd['bottom'] = max([wrd_hz[len(wrd_hz) - 1]['bottom'], wrd['bottom']])
                            wrd_hz.pop(-1)
                            wrd_hz.append(wrd)
                    else:
                        wrd_hz.append(wrd)
            return wrd_hz

        def key_right_phase2(key, words_):
            words = copy.deepcopy(words_)

            values = [i for i in words if i['top'] > key['top']-0.5
                    and i['top'] < key['bottom'] + 0.5 and 
                    i['x0'] > key['x1']]
            right_match = ''
            diff_val = 5000
            for value in values:
                diff = abs(key['x1'] - value['x0'])
                if diff < diff_val:
                    right_match = value
                    diff_val = diff
            if len(right_match) > 0:
                return right_match
            else:
                return '-'

        def vertical_merge(words_, num_lst):
            words = copy.deepcopy(words_)
            result = []
            for word in words:
                if len(result) == 0 or word['text'] in num_lst:
                    result.append(word)
                else:
                    diff_ver = abs(result[len(result)-1]['bottom']-word['top'])
                    diff_hor = abs(result[len(result)-1]['x0'] - word['x0'])
                    if diff_ver < 3 and diff_hor < 3:
                        word['text'] = result[len(result) - 1]['text']+' '+word['text']
                        word['x0'] = min([result[len(result) - 1]['x0'], word['x0']])
                        word['x1'] = max([result[len(result) - 1]['x1'], word['x1']])
                        word['top'] = min([result[len(result) - 1]['top'], word['top']])
                        word['doctop'] = min([result[len(result) - 1]['doctop'], word['doctop']])
                        word['bottom'] = max([result[len(result) - 1]['bottom'], word['bottom']])
                        result.pop(-1)
                        result.append(word)

                    else:
                        result.append(word)
            return result

        # x0_sec, x0_first
        def key_down_3(numeric_right_full_values, numerical_values_belowheader, x0_first, x0_sec):
            keys = copy.deepcopy(numeric_right_full_values)
            data = copy.deepcopy(numerical_values_belowheader)
            cnt = 0
            result = {'Key':[],
                    'Values':[]}
            top_final = ''
            for key in keys:
                diff_chk = key['x0'] - x0_first
                if 'Morethanoneactivityforat-riskpurposes*' in key['text'].replace(' ',''):
                    top_final = key['top']
                try:
                    next_vlu = keys[cnt+1]
                    if next_vlu == key:
                        next_vlu = keys[cnt+2]
                except Exception as err:
        #             print('error is:', err)
                    next_vlu = {'top':top_final}

                if diff_chk < 10:
                    next_vlu = keys[cnt+1]
                    down = list(filter(lambda x: x['top'] > key['bottom']
                                and x['bottom'] <= next_vlu['top'] and
                                    x['x0'] < x0_sec-15, data))
                else:
                    down = list(filter(lambda x: x['top'] > key['bottom']
                                and x['bottom'] <= next_vlu['top'] and 
                                    x['x1'] > x0_sec-10 and x['x0'] > key['x0']-16, data))

                down = sorted(down, key = lambda i: i['top'], reverse = False)
                if len(down) == 1:
                    result['Key'].append(key['text'])
                    result['Values'].append(down[0]['text'])

                elif len(down) >=2:
                    cnt_d = 0
                    down_single = []
                    for vlu in down:

                        if len(down_single) == 0:
                            down_single.append(vlu)

                        else:
                            diff_val_hz = abs(down_single[len(down_single)-1]['x1'] - vlu['x0'])
                            diff_val_vt = abs(down_single[len(down_single)-1]['top'] - vlu['top'])

                            if diff_val_vt > 3:

                                down_single = sorted(down_single, key = lambda i: i['x0'], reverse = False)
                                if len(down_single) == 1:
                                    result['Key'].append(key['text'])
                                    result['Values'].append(down_single[0]['text'])
                                    continue
                                else:
                                    multi = []
                                    for idx in range(len(down_single)):
                                        if len(multi) == 0:
                                            multi.append(down_single[idx])
                                        else:
                                            diff_val_hz_final = abs(multi[len(multi)-1]['x1'] - down_single[idx]['x0'])
                                            if diff_val_hz_final < 16:
                                                down_single[idx]['text'] = multi[len(multi)-1]['text'] + ' ' + down_single[idx]['text']
                                                down_single[idx]['x0'] = min(multi[len(multi)-1]['x0'] , down_single[idx]['x0'])
                                                down_single[idx]['x1'] = max(multi[len(multi)-1]['x1'] , down_single[idx]['x1'])
                                                multi.pop(-1)
                                                multi.append(down_single[idx])
                                            else:
                                                multi.append(down_single[idx])
                                    multi = [i['text'] for i in multi]

                                    if len(multi) == 1:
                                        result['Key'].append(key['text'])
                                        result['Values'].append(multi[0])
                                    else:
                                        result['Key'].append(f"""{key['text']} | {multi[0]}""")
                                        result['Values'].append(multi[1])
                                down_single = [vlu]
                            elif diff_val_hz < 16 and diff_val_vt < 0.7:
                                vlu['text'] = down_single[len(down_single) - 1]['text'] + ' '+ vlu['text']
                                vlu['x0'] = min(down_single[len(down_single) - 1]['x0'], vlu['x0'])
                                vlu['x1'] = max(down_single[len(down_single) - 1]['x1'], vlu['x1'])
                                vlu['top'] = min(down_single[len(down_single) - 1]['top'], vlu['top'])
                                vlu['bottom'] = max(down_single[len(down_single) - 1]['bottom'], vlu['bottom'])

                                down_single.pop(-1)

                                down_single.append(vlu)

                            else:
                                down_single.append(vlu)

                        cnt_d += 1
                    down_single = sorted(down_single, key = lambda i: i['x0'], reverse = False)
                    if len(down_single) == 1:
                        result['Key'].append(key['text'])
                        result['Values'].append(down_single[0]['text'])
                        continue
                    else:
                        multi = []
                        for idx in range(len(down_single)):
                            if len(multi) == 0:
                                multi.append(down_single[idx])
                            else:
                                diff_val_hz_final = abs(multi[len(multi)-1]['x1'] - down_single[idx]['x0'])
                                if diff_val_hz_final < 16:
                                    down_single[idx]['text'] = multi[len(multi)-1]['text'] + ' ' + down_single[idx]['text']
                                    down_single[idx]['x0'] = min(multi[len(multi)-1]['x0'] , down_single[idx]['x0'])
                                    down_single[idx]['x1'] = max(multi[len(multi)-1]['x1'] , down_single[idx]['x1'])
                                    multi.pop(-1)
                                    multi.append(down_single[idx])
                                else:
                                    multi.append(down_single[idx])
                        multi = [i['text'] for i in multi]
                        if len(multi) == 1:
                            result['Key'].append(key['text'])
                            result['Values'].append(multi[0])
                        else:
                            result['Key'].append(f"""{key['text']} | {multi[0]}""")
                            result['Values'].append(multi[1])
                else:
                    result['Key'].append(key['text'])
                    result['Values'].append('-')
                cnt += 1
            return result

        def part3_table_extractor(pdf_path):
            with pdfplumber.open(pdf_path) as pdf:
                pdf_pages = len(pdf.pages)
                global pdf_words_init
                pdf_words_init = [pdf.pages[page].extract_words(use_text_flow=False,keep_blank_chars=True,y_tolerance = 0, x_tolerance = 0) for page in range(pdf_pages)]
            num_lst = ['1', '2', '3', '4', '4a', '4b', '4c', '5', '5a', '5b', '5c','6', '6a', '6b', 
                    '6c', '7', '8', '8a', '8b', '8c', '9', '9a', '9b', '9c', '10', '11', '12',
                    '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']

            final_result = {}
            page_doc = []
            final_page_cnt = 0
            for all_words in pdf_words_init:
                final_page_cnt += 1
                all_words_new = less_horizontal_merge(all_words, num_lst)
                all_words_new = mean_creator(all_words_new)
        #         print('\nall_words_new values are:', all_words_new,'\n')
                part_3match = list(filter(lambda x: 'PartIII' in x['text'].replace(' ',''),all_words_new))

                if len(part_3match) > 0:
                    page3_check_values = check_box_values[final_page_cnt - 1]
                    part_3match = part_3match[0]
                    srch_wrds = vertical_over_flow(list(filter(lambda x: x['bottom'] > part_3match['top'] and
                                                            x['x1'] > part_3match['x1'] and
                                                            x['top'] > part_3match['top']-5, all_words_new)))

                    part3_head_ = list(filter(lambda x: x['bottom'] > part_3match['top'] and
                                            x['x0'] > part_3match['x1'] and
                                            x['top'] < part_3match['bottom'], srch_wrds))
        #             print('part3_head_ value is:', part3_head_)
                    ##############################################################
                    # part3 header full value
                    if len(part3_head_) > 0:
                        part3_head_1 = part3_head_[0]
                        part3_head_2 = list(filter(lambda x: part3_head_1['x0'] < x['x_mean'] < part3_head_1['x1'] and
                                                    x['top'] > part3_head_1['bottom'] and
                                                    abs(part3_head_1['x0'] - x['x0']) < 0.5, srch_wrds))
                        if len(part3_head_2) > 0:
                            part3_head_ = dict_concatenator(part3_head_1, part3_head_2[0])
                            part3_head_['text'] = part3_head_['text'].replace('and','&').replace(', ',',').strip()
                        else:
                            part3_head_ = part3_head_[0]

                    part3_match_ = copy.deepcopy(part_3match)

                    final_bottom = list(filter(lambda x: 'Morethanoneactivityforpassiveactivitypurposes*' in x['text'].replace(' ',''), all_words_new))
                    numerical_values_v = [i for i in all_words_new if i['x0'] > part_3match['x0']-15 and i['top'] >= part_3match['top']
                                        and i['bottom'] <= final_bottom[0]['bottom']+2]

                    numerical_values = vertical_merge(numerical_values_v, num_lst)
        #             print('\n numerical values are:', numerical_values)
                    numerical_values_belowheader = [i for i in numerical_values_v if i['top'] > part_3match['top']+2]
                    num_key_match_ = list(filter(lambda x: x['text'].strip() in num_lst, numerical_values))
                    x0_sec = sorted(num_key_match_, key = lambda i: i['x1'], reverse = True)[0]['x0']
                    x0_first = sorted(num_key_match_, key = lambda i: i['x0'], reverse = False)[0]['x0']

                    form_ = list(filter(lambda x: 'form' in joiner(x['text']).lower(), all_words_new))
                    if len(form_) > 0:
                        form = copy.deepcopy(form_[0]['text'])
                        form += '_page:' + str(final_page_cnt)
                    else:
                        form = '_page:' + str(final_page_cnt)
                    part_3_left = [i for i in num_key_match_ if i['x1'] > x0_first and i['x1'] < x0_sec-15]
                    part_3_right = [i for i in num_key_match_ if i['x1'] > x0_sec and i['x0'] > x0_sec -15]
                    num_key_match = part_3_left + part_3_right

                    numeric_right_values = []
                    numeric_right_full_values = []
                    for txt in num_key_match:
                        kr_value = key_right_phase2(txt, numerical_values)
                        if len(page3_check_values) > 0:
                            match_3 = list(filter(lambda x: joiner(only_alpha(x)) in joiner(only_alpha(kr_value['text'])), page3_check_values))
                            if len(match_3) > 0:
                                kr_value['text'] = ' X '+ kr_value['text']

                        numeric_right_values.append({txt['text']:kr_value['text']})
                        txt_ = copy.deepcopy(txt)
                        txt_['text'] = txt_['text'] + ' '+ kr_value['text']
                        txt_['x0'] = min(txt_['x0']-5, kr_value['x0']-5)
                        txt_['x1'] = max(txt_['x1'], kr_value['x1'])
                        txt_['top'] = min(txt_['top'], kr_value['top'])
                        txt_['doctop'] = min(txt_['doctop'], kr_value['doctop'])
                        txt_['bottom'] = max(txt_['bottom'], kr_value['bottom'])
                        numeric_right_full_values.append(txt_)
                    # 16 and 18 text hard coding
                    numeric_right_full_values = h_coding('ScheduleK-3isattachedif', 'checked.|', numeric_right_full_values, numerical_values_belowheader)
                    numeric_right_full_values = h_coding('Tax-exemptincomeand', 'nondeductibleexpenses', numeric_right_full_values, numerical_values_belowheader)
        #             print('numeric_right_full_values are:\n', numeric_right_full_values)
                    result = key_down_3(numeric_right_full_values, numerical_values_belowheader, x0_first, x0_sec)
                    final_result[form] = result
                    page_doc.append([final_page_cnt, form])

                else:
                    continue

            return pdf_pages, page_doc, final_result, part3_match_, part3_head_

        def h_coding(txt_1,txt_2, numeric_right_full_values, numerical_values_belowheader):
            first = list(filter(lambda x: txt_1 in x['text'].replace(' ',''), numeric_right_full_values))
            second = list(filter(lambda x: txt_2.replace('|','').strip() in x['text'].replace(' ',''), numerical_values_belowheader))

            if len(first) > 0 and len(second) > 0:
                second = second[0]
                first = first[0]
                if '|' in txt_2:
                    second['text'] = first['text']+'\n'+txt_2+'->'
                elif joiner(txt_2) in joiner(first['text']):
                    return numeric_right_full_values
                else:
                    second['text'] = first['text']+'\n'+second['text']
                second['x0'] = min(first['x0'], second['x0'])
                second['x1'] = max(first['x1'], second['x1'])
                second['top'] = min(first['top'], second['top'])
                second['bottom'] = max(first['bottom'], second['bottom'])
                numeric_right_full_values[numeric_right_full_values.index(first)] = second
            else:
                return numeric_right_full_values
            return numeric_right_full_values

        def horizontal_over_flow(wrds_, rng = 12, sub_rng = 1.5):
            wrds_all = copy.deepcopy(wrds_)
        #     print('wrds_all 4th page:', wrds_all[3])
            all_ = []
            for wrds_ in wrds_all:
                single_ = []
                wrds = copy.deepcopy(wrds_)
                for wrd in wrds:
                    if len(single_) == 0:
                        single_.append(wrd)
                    else:
                        if len(wrd['text'].strip()) == 0:
                            continue
                        diff_val = abs(single_[len(single_)-1]['x1'] - wrd['x0'])
                        if diff_val < rng:
                            if diff_val < sub_rng:
                                wrd['text'] = single_[len(single_)-1]['text']+wrd['text']
                            else:
                                wrd['text'] = single_[len(single_)-1]['text']+' ' + wrd['text']
                            wrd['x0'] = min([wrd['x0'], single_[len(single_)-1]['x0']])
                            wrd['x1'] = max([wrd['x1'], single_[len(single_)-1]['x1']])
                            wrd['top'] = min([wrd['top'], single_[len(single_)-1]['top']])
                            wrd['bottom'] = max([wrd['bottom'], single_[len(single_)-1]['bottom']])
                            single_.pop(-1)
                            single_.append(wrd)

                        else:
                            single_.append(wrd)

                all_.append(single_)
            return all_

        def vertical_over_flow(wrds_, rng = 2, space_ = ' '):
            wrds = copy.deepcopy(wrds_)
            result = []
            for wrd in wrds:
                if len(result) == 0:
                    result.append(wrd)
                else:
                    diff = abs(result[len(result) - 1]['bottom'] - wrd['top'])
                    diff_vert = wrd['x_mean'] in range(int(result[len(result) -1]['x0']), int(result[len(result)-1]['x1']))
        #             print('prev value is:', result[len(result)-1]['text'])
        #             print('present value is:', wrd['text'])
                    if diff < rng and diff_vert:
        #                 print('condition satisfied)')
                        vlu_ = dict_concatenator(result[len(result) - 1], wrd, space_)
                        result.pop(-1)
                        result.append(vlu_)
                    else:
        #                 print('condition not satisfied')
                        result.append(wrd)
            return result

        # def vertical_over_flow(wrds_, rng = 3):
        #     wrds = copy.deepcopy(wrds_)
        #     result = []
        #     for wrd in wrds:
        #         if len(wrd['text'].strip()) == 0:
        #             continue
        #         else:
        #             cond_match = list(filter(lambda x: x['x0'] >= wrd['x0']-1 and
        #                                     x['top'] > wrd['bottom'] and
        #                                     x['x0'] < wrd['x1'] and
        #                                     abs(wrd['bottom'] - x['top']) < rng,wrds))
        #             if len(cond_match) > 0:
        #                 cond_match = cond_match[0]
        #                 indx = wrds.index(cond_match)
        #                 wrd['text'] = wrd['text'] + ' '+ cond_match['text']
        #                 wrd['x0'] = min([wrd['x0'], cond_match['x0']])
        #                 wrd['x1'] = max([wrd['x1'], cond_match['x1']])
        #                 wrd['top'] = min([wrd['top'], cond_match['top']])
        #                 wrd['bottom'] = max([wrd['bottom'], cond_match['bottom']])
        #                 if len(result) > 0:
        #                     result.pop(-1)
        #                 result.append(wrd)
        #                 wrds[indx]['text'] = '' 

        #             else:
        #                 result.append(wrd)
        #     return result

        def dict_concatenator(wrd1, wrd2, space_ = ' '):
            result = copy.deepcopy(wrd1)
            result['text'] = wrd1['text'] + space_ + wrd2['text']
            result['x0'] = min([wrd1['x0'], wrd2['x0']])
            result['x1'] = max([wrd1['x1'], wrd2['x1']])
            result['top'] = min([wrd1['top'], wrd2['top']])
            result['bottom'] = max([wrd2['bottom'], wrd1['bottom']])
            result['x_mean'] = int((wrd2['x_mean']+ wrd1['x_mean'])/2)
            result['y_mean'] = int((wrd2['y_mean'] + wrd1['y_mean'])/2)

            return result

        def paranthesis_chk(wrds_):
            wrds = copy.deepcopy(wrds_)
            rst_ = []
            for wrd in wrds:
                if len(rst_) == 0:
                    rst_.append(wrd)
                else:
                    if joiner(wrd['text']) == '(' or joiner(wrd['text']) == '$(':
                        # print('para start...........\n', wrd['text'])
                        wrd_idx = wrds.index(rst_[len(rst_)-1])+1
                        wrd_chk = list(filter(lambda x: ')' in joiner(x['text']), wrds[wrd_idx+1:wrd_idx+4]))
                        # print('para wrd_chk value is:', wrd_chk)
                        if len(wrd_chk) > 0:
                            match_idx = wrds.index(wrd_chk[0], wrd_idx)
                            if match_idx - wrd_idx == 1:
                                vlu_ = dict_concatenator(wrd, wrds[match_idx])
                                wrds.pop(match_idx)
                                rst_.append(vlu_)
                            else:
                                wrds_to_concat = wrds[wrd_idx+1:match_idx+1]
                                in_para = ' '.join([i['text'] for i in wrds[wrd_idx+1:match_idx]])
                                wrd['text'] += in_para
                                vlu_ = dict_concatenator(wrd, wrds[match_idx])
                                rst_.append(vlu_)
                                [wrds.pop(wrds.index(i)) for i in wrds_to_concat]
                            # print('para value is:', vlu_)
                            # print('para....................end\n')
                        else:
                            rst_.append(wrd)
                    else:
                        rst_.append(wrd)
            return rst_


        def horiz_concat(wrds_,spcl_cond = 'None', rng = 8.5):
            wrds = copy.deepcopy(wrds_)
            # print('wrds before para is:', [i['text'] for i in wrds])
            wrds = paranthesis_chk(wrds)
            # print('para completed wrds are:', wrds)
            result = []
            for wrd in wrds:
                if len(wrd['text'].strip()) == 0:
                    continue
                if len(result) == 0:
                    result.append(wrd)
                else:
                    diff = abs(result[len(result) - 1]['x1'] - wrd['x0'])
                    diff_start = abs(result[len(result) - 1]['x1'] - wrd['x1'])
                    diff_vert = abs(result[len(result) - 1]['bottom'] - wrd['bottom'])
        #             print('\n')
        #             print('--'*85)
        #             print('prev value:', result[len(result) - 1]['text'])
        #             print('present value:', wrd['text'])
        #             print('diff value is:', diff)
                    if spcl_cond != 'None' and joiner(result[len(result) - 1]['text']) == joiner('X'):
                        if wrd['text'].split()[0] == 'X':
                            vlu_ = wrd
                        else:
        #                 print('1')
                            vlu_ = dict_concatenator(result[len(result) - 1], wrd)
                        result.pop(-1)
                        result.append(vlu_)
                    elif spcl_cond != 'None' and diff_start < rng < diff and diff_vert < 1.2:
        #                 print('2')
        #                 if diff_vert < 1.2:
                        vlu_1 = copy.deepcopy(result[len(result) - 1])
                        vlu_1['text'] = vlu_1['text'].replace(')','')+ ' '+ wrd['text'] + ')'
                        result.pop(-1)
                        result.append(vlu_1)
        #                 else:
        #                     result.append(wrd)
                        # print('***'* 85)
                        # print(vlu_1['text'])
                    elif spcl_cond != 'None' and joiner(wrd['text']) == '%':
        #                 print('3')
                        num_chk = [i for i in result[len(result)-1]['text'] if i.isnumeric()]
                        # print('%num_chk text:', result[len(result)-1]['text'])
                        # print('%num_chk value is:', num_chk, '\n','--'*85)
                        if len(num_chk) > 0:
                            vlu_ = dict_concatenator(result[len(result)-1], wrd)
                            # print('vlu_after concat is:', vlu_['text'])
                            result.pop(-1)
                            result.append(vlu_)
                        else:
                            result.append(wrd)
                    elif spcl_cond != 'None' and '$' in joiner(wrd['text']) and \
                    '$' in joiner(result[len(result) - 1]['text']):
        #                 print('4')
                        result.append(wrd)
                    elif diff < rng:
        #                 print(5)
                        vlu_ = dict_concatenator(result[len(result) - 1], wrd)
                        result.pop(-1)
                        result.append(vlu_)
                    else:
        #                 print('6-else')
                        result.append(wrd)
            return result

        def vertic_match_concat(wrds_, all_wrds_, rng = 2):
            all_wrds = copy.deepcopy(all_wrds_)
            wrds = copy.deepcopy(wrds_)
            result = []
            for wrd in wrds:
                down_vlu = list(filter(lambda x: x['top'] > wrd['bottom'] and
                                    wrd['x0'] < x['x_mean'] < wrd['x1'] and
                                    abs(wrd['bottom'] - x['top']) < rng, all_wrds))

                if len(down_vlu) > 0:
                    down_vlu = down_vlu[0]
                    vlu_ = dict_concatenator(wrd, down_vlu)
                    result.append(vlu_)
                    all_wrds_.pop(all_wrds_.index(down_vlu))

                else:
                    result.append(wrd)
            return result

        def dict_text_ext(list_dict):
            wrds = copy.deepcopy(list_dict)
            return [i['text'] for i in wrds]


        def sub_table_ext(pdf_pages, result, nxt_vlu, form, master_data):
            dummy = pd.DataFrame({'Key':[' ']})
            final_result = pd.DataFrame()
            for key in result['Key']:
                value_ = result.Values[result.Key == key].tolist()
                if len(value_) > 0:
                    value_ = value_[0]
                else:
                    value_ = '-'
                reslt = {'Key':[key], 'Values':[value_]}
    #                 print('reslt value is:', reslt)
                if '|' in key:
                    box_, code_ = key.split('|')
                    box = box_.split(' ')[0]
                    code = ''.join([i for i in code_ if i.isalpha()])
                    header_ = master_data[master_data.Values == box+code]['Header'].tolist()
        #             print('--'*85)
        #             print('\nheader_ value is:', header_)
        #             print('code value is:', code)
        #             print('--'*85)
                    if len(header_) > 0:
                        header_ = header_[0]
                        if len(code) > 0:
                            header = f'{header_}, BOX {box}, CODE {code}'
                        else:
                            if joiner(code_) == joiner('*'):
                                header = f'{header_}, BOX {box}'
                            else:
                                header = f'{header_}, BOX {box}, CODE {code}'
        #                 print('\n')
        #                 print('value extraction inputs'+'--'*25)
        #                 print('pdf_pages value is:', pdf_pages)
        #                 print('header value is:', header)
        #                 print('nxt_vlu is:', nxt_vlu)
        #                 print('form value is:', form)
                        df = value_extractor(pdf_pages, header, nxt_vlu, form)
                        columns = df.columns.tolist()
                        if 'Warning' in columns:
                            final_result = concatenator([final_result, reslt])
                            continue
                        else:
                            final_result = concatenator([final_result,dummy, reslt])
                            final_result = concatenator([final_result, df, dummy])
                    else:
                        final_result = concatenator([final_result, reslt])
                else:
                    final_result = concatenator([final_result, reslt])
            final_result.reset_index(drop = True, inplace = True)
    #             print('\ntable final result value is:', final_result.to_dict())
            return final_result

        def part_3_table_subtable(pdf_path):
            pdf_pages,face_page, part3_list, match_, head_ = part3_table_extractor(pdf_path)
            final_result = []
            if len(face_page) > 0:
                for idx in range(len(face_page)):

                    nxt_idx = idx + 1
                    if nxt_idx < len(face_page):
                        if idx > 0:
    #                             print('\n\n\ncolumn values:', final_result[0].columns,'\n\n')
                            clmn_vlu_ = final_result[0].columns
                            clmn_vlus = {}
                            for clm_vlu in clmn_vlu_:
                                if joiner(clm_vlu) == joiner('Key'):
                                    clmn_vlus[clm_vlu] = match_['text']
                                elif joiner(clm_vlu) == joiner('Values'):
                                    clmn_vlus[clm_vlu] = head_['text']
                                else:
                                    clmn_vlus[clm_vlu] = clm_vlu
                            vlu_with_nxt_clmn = concatenator([final_result[-1], pd.DataFrame([clmn_vlus])])
                            final_result.pop(-1)
                            final_result.append(vlu_with_nxt_clmn)
                        nxt_vlu = face_page[nxt_idx][0]
                        form = face_page[idx][1]
                        sub_right = sub_right_keys(part3_list[form], master_data, nxt_vlu, form)
                        sub_right_df = pd.DataFrame(sub_right)
                        sub_table_ = sub_table_ext(pdf_pages,sub_right_df, nxt_vlu, form, master_data)
                        part1_table = part1_extraction(form, match_, pdf_pages, nxt_vlu)
                        part1_clmns = [' '+' '*i for i in range(len(part1_table.columns.tolist()))]
                        part1_clmns[:2] = part1_table.columns.tolist()[:2]
                        part1_table.columns = part1_clmns
                        dummy_clmn = pd.DataFrame([''], columns = [' ']).reset_index(drop=True)
                        sub_table = pd.concat([pd.DataFrame(part1_table),dummy_clmn, sub_table_.reset_index(drop = True)], axis = 1)
                        sub_table['Form'] = form
                        final_tab = pd.concat([sub_table['Form'], sub_table.drop('Form', axis = 1)], axis = 1)
                        final_result.append(final_tab)

                    else:
                        if idx > 0:
    #                             print('\n\n\ncolumn values:', final_result[0].columns,'\n\n')
                            clmn_vlu_ = final_result[0].columns
                            clmn_vlus = {}
                            for clm_vlu in clmn_vlu_:
                                if joiner(clm_vlu) == joiner('Key'):
                                    clmn_vlus[clm_vlu] = match_['text']
                                elif joiner(clm_vlu) == joiner('Values'):
                                    clmn_vlus[clm_vlu] = head_['text']
                                else:
                                    clmn_vlus[clm_vlu] = clm_vlu
    #                             print('clmn vlu is:', clmn_vlus)
                            vlu_with_nxt_clmn = concatenator([final_result[-1], pd.DataFrame([clmn_vlus])])
                            final_result.pop(-1)
                            final_result.append(vlu_with_nxt_clmn)
    #                             print('final_result value is:', final_result)
                        nxt_vlu = len(pdf_words_init)
                        form = face_page[idx][1]
                        sub_right = sub_right_keys(part3_list[form], master_data, nxt_vlu, form)
                        sub_right_df = pd.DataFrame(sub_right)
                        sub_table_ = sub_table_ext(pdf_pages, sub_right_df, nxt_vlu, form, master_data)
                        part1_table = part1_extraction(form, match_, pdf_pages, nxt_vlu).reset_index(drop = True)
                        part1_clmns = [' '+' '*i for i in range(len(part1_table.columns.tolist()))]
                        part1_clmns[:2] = part1_table.columns.tolist()[:2]
                        part1_table.columns = part1_clmns
                        dummy_clmn = pd.DataFrame([''], columns = [' ']).reset_index(drop=True)
                        sub_table = pd.concat([pd.DataFrame(part1_table),dummy_clmn, sub_table_.reset_index(drop = True)], axis = 1)
                        sub_table['Form'] = form
                        final_tab = pd.concat([sub_table['Form'], sub_table.drop('Form', axis = 1)], axis = 1)
                        final_result.append(final_tab)
            final_result_ = concatenator(final_result).fillna(' ')
            final_result_.reset_index(drop=True, inplace = True)
            clmn_values_ = final_result_.columns.tolist()
            final_result_.loc[final_result_[clmn_values_[1]] == ' ', 'Form'] = final_result_[clmn_values_[1]]
            final_result_.rename(columns = {'Key':match_['text'], 'Values':head_['text']}, inplace = True)
            final_result_ = final_result_
            return final_result_, match_, head_

        def mean_creator(lst_wrds_):
            '''This function takes single list of dictionaries as input and create x_mean and y_mean values for 
            every dictionary element in the given list based on x0, x1 and top, bottom values respectively 
            and inserts these x_mean and y_mean key values to the corresponding words. This function returns these
            collection of modified words in the list of dictionaries format.'''
            lst_wrds = copy.deepcopy(lst_wrds_)
            result = []
            for wrds in lst_wrds:
                wrds['x_mean'] = int((wrds['x0'] + wrds['x1'])/2)
                wrds['y_mean'] = int((wrds['top'] + wrds['bottom'])/2)
                result.append(wrds)
            return result  

        def words_sort12(wrds_):
            wrds_all = copy.deepcopy(wrds_)
            issue_values = list(filter(lambda x: (x['bottom'] - x['top']) > 20, wrds_all))
            for vlu in issue_values:
                vlu_num_chk = joiner(vlu['text'])[:-1]
                if vlu_num_chk.isnumeric():
                    prv_wrd = wrds_all[wrds_all.index(vlu)-1]
                    vlu['text'] = vlu['text'][:-1]
                    vlu['top'] = prv_wrd['top']
                    vlu['bottom'] = prv_wrd['bottom']
                    continue
                wrds_all[wrds_all.index(vlu)]['text'] = '->'
            wrds_all = mean_creator(wrds_all)
            final_wrds = []
            wrd_chk = [{'text':'initial','bottom':0}]
            for wrd in wrds_all:
                if wrd['text'] == '->':
                    continue
                if wrd['top'] > wrd_chk[len(wrd_chk)-1]['bottom']:
                    line_match = list(filter(lambda x: wrd['top'] < x['y_mean'] and
                                            x['y_mean'] < wrd['bottom'], wrds_all))
                    line_match_chk = []
                    dummy_lst = []
                    for vlu in line_match:
                        if len(dummy_lst) == 0:
                            dummy_lst.append(vlu)
                        else:
                            diff_val = abs(dummy_lst[len(dummy_lst) - 1]['bottom'] - vlu['bottom'])
                            if diff_val > 10:
                                line_match_chk.append(vlu)
                            else:
                                continue
                    if len(line_match_chk) == 0:
                        line_match_sort = sorted(line_match, key = lambda i: i['x0'], reverse = False)
                    else:
                        line_match_sort = words_sort12(line_match_chk)
                    for sort_word in line_match_sort:
                        final_wrds.append(sort_word)
                        wrd_chk.append(wrd)

                else:
                    continue
            return final_wrds

        def words_sort(wrds_lst):
            '''This function takes single list of dictionaries as input and for every element in the list it searches
            the same axis/row elements and sorts them based on x0 value of elements in ascending order and inserts them into
            the final_wrds variable.This function returns the final_wrds value which is of list of dictionary format.'''
            wrds_ = copy.deepcopy(wrds_lst)
            wrds = mean_creator(wrds_)
            extra_wrds = []
        #     print('wrds value is:', wrds, end = ' ')
            final_wrds = []
            for wrd in wrds:
                if wrd not in extra_wrds:
                    # print('wrd value is:', wrd['text'])
                    same_axis = list(filter(lambda x: x['bottom'] > wrd['top'] and x['y_mean'] < wrd['bottom'] and x not in extra_wrds, wrds))
        #             same_axis = list(filter(lambda x: x['y_mean'] in range(int(wrd['top']-1), int(wrd['bottom']+1)), wrds))
                    axis_sort = sorted(same_axis, key = lambda i: i['x0'], reverse = False)
                    # print('words:',[i['text'] for i in axis_sort])
                    final_wrds.extend(axis_sort)
        #             print('words value is:', [i['text'] for i in wrds])
                    extra_wrds.extend(axis_sort)
                else:
                    pass
                    # print('else:',wrd['text'])
            return final_wrds

        def horizontal_part1(wrds_, rng = 1.5, sub_rng = 0.9):
            # rng = 10.7 bcz part ii is merging with its right
            wrds_all = copy.deepcopy(wrds_)
            wrds_all = words_sort(wrds_all)
            result = []
            for wrd in wrds_all:

                if len(result) == 0:
                    result.append(wrd)
                else:
                    diff_val = abs(result[len(result)-1]['x1'] - wrd['x0'])
                    if diff_val < rng:
                        if diff_val < sub_rng:
                            wrd['text'] = result[len(result)-1]['text']+wrd['text']
                        else:
                            wrd['text'] = result[len(result)-1]['text']+' ' + wrd['text']
                        wrd['x0'] = min([wrd['x0'], result[len(result)-1]['x0']])
                        wrd['x1'] = max([wrd['x1'], result[len(result)-1]['x1']])
                        wrd['top'] = min([wrd['top'], result[len(result)-1]['top']])
                        wrd['bottom'] = max([wrd['bottom'], result[len(result)-1]['bottom']])
                        result.pop(-1)
                        result.append(wrd)

                    else:
                        result.append(wrd)

            return result

        def horizontal_part12(wrds_, rng = 1.5, sub_rng = 0.9):
            # rng = 10.7 bcz part ii is merging with its right
            wrds_all = copy.deepcopy(wrds_)
            wrds_all = words_sort12(wrds_all)
            result = []
            for wrd in wrds_all:

                if len(result) == 0:
                    result.append(wrd)
                else:

                    diff_val = abs(result[len(result)-1]['x1'] - wrd['x0'])
                    val_chk = ''.join([i for i in wrd['text'] if i.isnumeric()])
                    if joiner(wrd['text']) == '$':
                        result.append(wrd)
                    elif joiner(result[len(result)-1]['text']) == '$' or joiner(result[len(result)-1]['text']) == '$(': 
                        if '$' not in wrd['text'] and len(val_chk) > 0:
                            wrd_vlu = dict_concatenator(result[len(result)-1], wrd, space_ = '')
                            wrd_vlu['x1'] -= 2
                            wrd_vlu['x_mean'] -= 2
                            result.pop(-1)
                            result.append(wrd_vlu)
                        elif '(' in wrd['text'] or '-' in wrd['text']:
                            wrd_vlu = dict_concatenator(result[len(result)-1], wrd, space_ = '')
                            wrd_vlu['x1'] -= 2
                            wrd_vlu['x_mean'] -= 2
                            result.pop(-1)
                            result.append(wrd_vlu)
                        else:
                            result[len(result)-1]['x1'] -= 2 
                            result.append(wrd)
                    elif diff_val < rng:
                        if diff_val < sub_rng:
                            wrd['text'] = result[len(result)-1]['text']+wrd['text']
                        else:
                            wrd['text'] = result[len(result)-1]['text']+' ' + wrd['text']
                        wrd['x0'] = min([wrd['x0'], result[len(result)-1]['x0']])
                        wrd['x1'] = max([wrd['x1'], result[len(result)-1]['x1']])
                        wrd['top'] = min([wrd['top'], result[len(result)-1]['top']])
                        wrd['bottom'] = max([wrd['bottom'], result[len(result)-1]['bottom']])
                        result.pop(-1)
                        result.append(wrd)

                    else:
                        result.append(wrd)

        #     print('\n\nresult value is:', result, '\n\n')
            return result


        def part1_extraction(form, match, pdf_pages, nxt_vlu):
            part1_2keys = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H1', 'H2', 'I1', 'I2', 'J', 'K', 'L', 'M', 'N']
            match_ = copy.deepcopy(match)
            front_page = int(form.split(':')[-1]) - 1
            all_words = pdf_words_init[front_page]
            wrds_bfr_part3 = horizontal_part1(list(filter(lambda x: x['x1'] < match_['x0'] - 1, all_words)), sub_rng = 0.9)
            right_boundary = copy.deepcopy(match_)['x0']-20
            page_check_values_1 = check_box_values[front_page]

            part1_header = list(filter(lambda x: joiner(x['text']) == joiner('PartI'), wrds_bfr_part3))
            final_df_12 = []
            if len(part1_header) > 0:
                part1_header = part1_header[0]

                part1_header_right = list(filter(lambda x: x['x0'] > part1_header['x1'] and
                                                part1_header['top'] < x['y_mean'] < part1_header['bottom'],wrds_bfr_part3))
                # print('--'*85)
                # print('wrds before part3:', [i['text'] for i in wrds_bfr_part3])
                if len(part1_header_right) > 0:
                    part1_header_right = part1_header_right[0]

                part2_header = list(filter(lambda x: joiner(x['text']) == joiner('PartII'), wrds_bfr_part3))
                if len(part2_header) > 0:
                    part2_header = part2_header[0]
                    part2_df = part2_extraction(front_page, match_,pdf_pages, nxt_vlu, form)
                    part2_columns = part2_df.columns.tolist()
                    new_part2_columns = copy.deepcopy(part2_columns)
                    new_part2_columns[:2] = [part1_header['text'], part1_header_right['text']]

                    part2_df.columns = new_part2_columns

                part2_header_right = list(filter(lambda x: x['x0'] > part2_header['x1'] and
                                                part2_header['top'] < x['y_mean'] < part2_header['bottom'],wrds_bfr_part3))[0]

                wrds_bf3_abv2_bel1 = list(filter(lambda x: x['top'] < part2_header['top'] and
                                                x['top'] > part1_header['top']+0.5, wrds_bfr_part3))
                # check box match
                part1_check_value_match = [i for i in page_check_values_1 
                                       if len(list(filter(lambda x: joiner(i) in joiner(x['text']), wrds_bf3_abv2_bel1))) > 0]
                if len(part1_check_value_match) > 0:
                    matched_value = list(filter(lambda x: joiner(part1_check_value_match[0]) in joiner(x['text']), wrds_bf3_abv2_bel1))
                    matched_value_idx = wrds_bf3_abv2_bel1.index(matched_value[0])
                    wrds_bf3_abv2_bel1[matched_value_idx]['text'] = 'X ' + wrds_bf3_abv2_bel1[matched_value_idx]['text']
                wrds_bf3_bel2 = list(filter(lambda x: x['top'] > part2_header['top']+0.5, wrds_bfr_part3))
                part1_lb = part1_header['x0'] - 20
                part2_lb = part2_header['x0'] - 20

                part1_keywrds = list(filter(lambda x: x['x0'] > part1_lb and
                                x['x1'] < part1_header['x1'] and
                                x['text'].strip() in part1_2keys and
                                x['bottom'] < part2_header['top'], wrds_bf3_abv2_bel1))
                # print('part1_keywrds are:', part1_keywrds)
                right_full_value = []
                for key in part1_keywrds:
                    key_right = list(filter(lambda x: x['x0'] > key['x1'] and
                                            key['top'] < x['y_mean'] < key['bottom'],wrds_bf3_abv2_bel1))
                    # print('\nkey_right values are:', key_right)
                    key_right = [i for i in key_right if '(cid:160)' not in i['text']]
                    if len(key_right) > 0:
                        key_right = key_right[0]
                        right_full_value.append(dict_concatenator(key, key_right, space_ = ' '))
                part_1final = {part1_header['text']:[], part1_header_right['text']:[]}
                cnt_ = 0
                for key_full in right_full_value:
                    cnt_ += 1
                    if cnt_ < len(right_full_value):
                        nxt_vlu = right_full_value[cnt_]
                    else:
                        nxt_vlu = part2_header
        #             print('\n wrds_bf3_abv2_bel1 is:', wrds_bf3_abv2_bel1)
                    down_vlu = list(filter(lambda x:x['y_mean'] < nxt_vlu['top'] and x['top'] > key_full['top'] and
                                        x['top'] < nxt_vlu['top']-0.5 and
                                            joiner(x['text']) not in joiner(key_full['text']), wrds_bf3_abv2_bel1))
                    # print('\nkey down vlu is:', down_vlu)
                    if len(down_vlu) == 1:
                        part_1final[part1_header['text']].append(key_full['text'])
                        part_1final[part1_header_right['text']].append(down_vlu[0]['text'])
                    elif len(down_vlu) > 1 and key_full['text'].split(' ')[0].strip() == 'C':
                        part_1final[part1_header['text']].append(key_full['text'])
                        part_1final[part1_header_right['text']].append(down_vlu[-1]['text'])
                    elif len(down_vlu) > 1:
                        part_1final[part1_header['text']].append(key_full['text'])
                        part_1final[part1_header_right['text']].append(' '.join([i['text'] for i in down_vlu]))
                    else:
                        key_right_val_chk = list(filter(lambda x: key_full['top'] < x['y_mean'] < key_full['bottom']
                                                        and x['x0'] > key_full['x1'],wrds_bf3_abv2_bel1))
                        if len(key_right_val_chk) > 0:
                            part_1final[part1_header['text']].append(key_full['text'])
                            part_1final[part1_header_right['text']].append(key_right_val_chk[-1]['text'])
                        else:
                            part_1final[part1_header['text']].append(key_full['text'])
                            part_1final[part1_header_right['text']].append(' ')
                part1_df = pd.DataFrame(part_1final).reset_index(drop = True)
                part1_clmns = part1_df.columns.tolist()
                dummy_df = pd.DataFrame({f'{part1_clmns[0]}':[' ',' ',f"{part2_header['text']}", ' '], f'{part1_clmns[1]}':[' ', ' ',f"{part2_header_right['text']}", ' ']})
                final_df_12.append(part1_df.reset_index(drop=True))
                final_df_12.append(dummy_df.reset_index(drop=True))                         
                final_df_12.append(part2_df.reset_index(drop = True))
                final_result = pd.concat(final_df_12, axis = 0).reset_index(drop=True)
                final_result.fillna(' ')
                return final_result

        def right_key_match(wrd, pg_no):
            wrd_ = copy.deepcopy(wrd)
            wrds_all = copy.deepcopy(pdf_words_init)
            wrds_ = [horizontal_part12(wrds, rng = 12, sub_rng = 3) for wrds in [wrds_all[pg_no]]]
        #     print('\n\nhoirzontal concat:', wrds_)
            wrds_ = [vertical_over_flow(wrds, rng = 3) for wrds in wrds_][0]
        #     print('\n\nvertical concat:', wrds_)
            result = list(filter(lambda x: joiner(wrd) == joiner(x['text']),wrds_))
        #     print('\nresult_value:', result)
            return result

        def sub_right_keys(part_3_dict, master_data, stp, form):
        #     print('\nstp value is:', stp, '\n')
        #     print('form value is:', form)
            pdf_words_start = copy.deepcopy(pdf_words_init)
            start_ = int(form.split(':')[-1].strip())-1
            pdf_words_start = pdf_words_start[start_:stp]
        #     pdf_words_ = horizontal_over_flow(pdf_words_start)
        #     print('pdf_words 4th page value is:', pdf_words_[3])
            pdf_words_ = [horizontal_part12(wrds, rng = 12, sub_rng = 3) for wrds in pdf_words_start]
            pdf_words_ = [vertical_over_flow(wrds, rng = 3) for wrds in pdf_words_]
        #     print('pdf_words 4th page value is:', pdf_words_[3])

        #     pdf_words_20 = copy.deepcopy(pdf_words_)
            result_ = copy.deepcopy(part_3_dict)
            result = pd.DataFrame(result_, index = [form for i in range(len(result_['Key']))])
            final_val = {'Key':[],'Values':[]}
            vlu_chk = []

            result_key = result['Key'].tolist()
        #     print('result_key value is:\n', result_key)
            last_cnt = -1
            for key in result_key:
                last_cnt += 1
        #         print('\n\n')
        #         print('final_val is:', final_val)
        #         print('\n\n')
                if '|' in key:
                    num_ = key.split(' ')[0]
        #             print('present_vlu:', num_)
                    if last_cnt+1 == len(result_key):
                        sm_vlu = 0.8
                    else:
        #                 print('next_vlu is:', result_key[result_key.index(key)+1].split(' ')[0])
                        sm_vlu = SM(None, num_, result_key[result_key.index(key)+1].split(' ')[0]).ratio()
        #             print('sm_vlu:', sm_vlu)

                    if sm_vlu != 1.0:
    #                         print('\nkey value is:', key)
                        final_val['Key'].append(key)
                        vlu = result.Values[result.Key == key].tolist()
    #                         print('vlu is:', vlu)
                        if len(vlu) > 0:
                            final_val['Values'].append(result.Values[result.Key == key].tolist()[0])
                        else:
                            final_val['Values'].append('-')
        #                 num_ = vlu_chk[len(vlu_chk) - 1]
                        vlu_chk.append(num_)
                        key_match = [i for i in master_data['Values'] if num_ in str(i)]

                        ref_key = ''
                        page_vlu = []
                        for key_m in key_match:

                            key_val_header = master_data.Header[master_data.Values == key_m].tolist()[0]

                            if str(key_m).isnumeric():
        #                         print('pdf_words_4th page:', pdf_words_[3], '\n\n')
                                ref_key = key_val_header
                                main_srch = f'LINE {key_m} OVERFLOW:'
                                page_srch_cnt = -1
                                page_length = len(pdf_words_)
                                for wrds_idx in range(len(pdf_words_start)):
                                    page_srch_cnt = wrds_idx
                                    page_srch = list(filter(lambda x: joiner(x['text']) == joiner(main_srch), pdf_words_[wrds_idx]))
                                    if len(page_srch) > 0:
                                        page_vlu.append(page_srch_cnt)
                                if len(page_vlu) == 0:
                                    break

    #                                 final_val['Key'].append(' ')
    #                                 final_val['Values'].append(' ')
        #                         print('page_vlu is:', page_vlu)
                                final_val['Key'].append(f'{main_srch} ---> Page_no: {start_ + page_vlu[-1]+1}')
                                final_val['Values'].append(' ')
        #                             if num_ == '20':
        #                                 print('result key values:', result['Key'].tolist())
        #                                 print('key_match', key_match)
        #                                 print('key_val_match is:', key_val_match)
        #                                 print('page_srch:', page_srch)
        #                                 print('page_match:', start_+page_srch_cnt+1)
        #                                 print('words value is:\n', pdf_words_20[3])
        #                                 print('\n')
                            else:
                                for page_srch_idx in page_vlu:
        #                             print(f'page_extraction is:{key_m} ', page_srch_idx)
                                    search_key = f'LINE {key_m} - {ref_key} - {key_val_header}'
                                    search_splt = search_key.split('\n')[0]
        #                             print('search_key value is:', search_splt)
                                    key_val_match = right_key_match(search_splt, page_srch_idx)
        #                             key_val_match = list(filter(lambda x: joiner(search_splt) == joiner(x['text']),pdf_words_[page_srch_idx]))

        #                             print('key_value match:', key_val_match)
        #                             print(f'pdf_word_with_idx{page_srch_idx}: ', [i['text'] for i in srch_wrds],'\n')
                                    if len(key_val_match) > 0:
                                        key_val_match_ = key_val_match[0]
                                        right_val = list(filter(lambda x: x['x0'] > key_val_match_['x1'] and 
                                                            x['bottom'] > key_val_match_['top'] and
                                                            x['top'] < key_val_match_['bottom'], pdf_words_[page_srch_idx]))
                                        final_val['Key'].append(search_key)
                                        if len(right_val) > 0:
                                            final_val['Values'].append(right_val[0]['text'])
                                        else:

                                            final_val['Values'].append('-')
        #                                 break


                    else:
                        final_val['Key'].append(key)
                        vlu = result.Values[result.Key == key].tolist()
                        if len(vlu) > 0:
                            final_val['Values'].append(result.Values[result.Key == key].tolist()[0])
                        else:
                            final_val['Values'].append('-')
                        continue

                else:
                    final_val['Key'].append(key)
                    vlu = result.Values[result.Key == key].tolist()
                    if len(vlu) > 0:
                        final_val['Values'].append(result.Values[result.Key == key].tolist()[0])
                    else:
                        final_val['Values'].append('-')
    #             print('final_val is:', final_val)
            return final_val    

        def only_alpha(wrd_):
            wrd = copy.deepcopy(wrd_)
            wrd = ''.join([i for i in wrd if i.isalpha()])
            return wrd
        def part2_extraction(face_page, match,pdf_pages, nxt_vlu, form):
            part1_2keys = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H1', 'H2', 'I1', 'I2', 'J', 'K', 'L', 'M', 'N']
            match_ = copy.deepcopy(match)
            all_words = pdf_words_init[face_page]
            wrds_bfr_part3 = horizontal_part12(list(filter(lambda x: x['x1'] < match_['x0']-5 , all_words)), rng = 2.0, sub_rng = 0.9)
            wrds_bfr_part3 = text_cleaner_part2(wrds_bfr_part3)
            page_check_values = check_box_values[face_page]
    #             print('\npage_check values is:', page_check_values)
        #     print('\nwrds_bfr_part3', wrds_bfr_part3, '\n')
            part2_header = list(filter(lambda x: joiner(x['text']) == joiner('PartII'), wrds_bfr_part3))
            part1_header = list(filter(lambda x: joiner(x['text']) == joiner('PartI'), wrds_bfr_part3))
            if len(part2_header) > 0:
                part2_header = part2_header[0]
                part2_header_right = list(filter(lambda x: x['x0'] > part2_header['x1'] and
                                                part2_header['top'] < x['y_mean'] < part2_header['bottom'],wrds_bfr_part3))
                if len(part2_header_right) > 0:
                    part2_header_right = part2_header_right[0]

                wrds_bf3_bel2 = list(filter(lambda x: x['top'] > part2_header['top']+0.5, wrds_bfr_part3))
                part2_lb = part2_header['x0'] - 20
                part2_keywrds = list(filter(lambda x: x['x0'] > part2_lb and
                                x['x1'] < part2_header['x1'] and
                                x['text'].strip() in part1_2keys, wrds_bf3_bel2))
                ending_clmn = list(filter(lambda x: joiner('Ending') in joiner(x['text']) and
                                                    x['top'] > part2_keywrds[-1]['bottom'], wrds_bfr_part3))
                if len(ending_clmn) > 0:
                    ending_clmn = ending_clmn[0]
                wrds_bf3_bel2_end = list(filter(lambda x: x['top'] < ending_clmn['bottom'], wrds_bf3_bel2))
            wrds_bf3_bel2_end = [i for i in wrds_bf3_bel2_end if i['text'] != '->']
            print('page_check_values are:', page_check_values)
            part2_check_value_match = [i for i in wrds_bf3_bel2_end
                                       if len(list(filter(lambda x: joiner(x) in joiner(i['text']), page_check_values))) > 0]
            print('part2_check_value_match', part2_check_value_match)
            if len(part2_check_value_match) > 0:
                for dict_vlu_2 in part2_check_value_match:
                    dict_wrd_idx = wrds_bf3_bel2_end.index(dict_vlu_2)
                    if joiner(wrds_bf3_bel2_end[dict_wrd_idx]['text'])[0] != 'x':
                        print('matched index values are:', wrds_bf3_bel2_end[dict_wrd_idx]['text'])
                        wrds_bf3_bel2_end[dict_wrd_idx]['text'] = 'X ' + wrds_bf3_bel2_end[dict_wrd_idx]['text']
            part2_key_right = []
            part2_key_fullvalue = {}
            for key_wrd in part2_keywrds:
                key_wrd_right = list(filter(lambda x: key_wrd['top'] < x['y_mean'] < key_wrd['bottom']
                                            and x['x0'] > key_wrd['x1'], wrds_bf3_bel2_end))

        #         print('\nkey_wrd value is:{} and key_wrd_right value is:{}'.format(key_wrd, key_wrd_right))
                if len(key_wrd_right) > 0:
                    if len(key_wrd_right) == 1:
                        key_wrd_right = key_wrd_right[0]

                        match_comb = list(filter(lambda x: joiner(only_alpha(x)) in joiner(only_alpha(key_wrd_right['text'])), page_check_values))
                        if len(match_comb) > 0:
                            if key_wrd_right['text'].strip()[0] != 'X':
                                key_wrd_right['text'] = ' X '+key_wrd_right['text']
                        key_wrd_vlu = dict_concatenator(key_wrd, key_wrd_right)
                        key_wrd_vlu['text'] = key_wrd_vlu['text'].replace('|','').strip()
                        part2_key_right.append(key_wrd_vlu)
                    elif len(key_wrd_right) > 1:
                        if len(key_wrd_right) == 2 and joiner(key_wrd_right[1]['text']) == 'seestatement':
                            key_wrd_vlu_1 = dict_concatenator(key_wrd, key_wrd_right[0])
                            key_wrd_vlu = dict_concatenator(key_wrd_vlu_1, key_wrd_right[1])
                            key_wrd_vlu['text'] = key_wrd_vlu['text'].replace('|','').strip()
                            part2_key_right.append(key_wrd_vlu)
                            continue

        #                 print('\nkey_wrd_right values are:', key_wrd_right)
                        key_wrd_hz = horiz_concat(key_wrd_right, spcl_cond = 'X')
        #                 print('\nkey_wrd_hz value is:', key_wrd_hz)
                        key_wrd_vt = vertic_match_concat(key_wrd_hz, wrds_bf3_bel2_end)
        #                 print('\nkey_wrd_vt value is:', key_wrd_vt)
        #                 cross_chk = list(filter(lambda x: joiner(x['text'])[:1] == 'x', key_wrd_vt))
                        wrd_len_chk = min([len(i['text']) for i in key_wrd_vt])
                        if len(key_wrd_vt) > 1 and wrd_len_chk > 2:
        #                     if len(cross_chk) == 1:
        #                         key_wrd_vt = cross_chk
                            part2_key_fullvalue[f"{key_wrd['text'].strip()}"] = [i['text'] for i in key_wrd_vt]
                            top_min = min([i['top'] for i in key_wrd_vt])
                            bottom_max = max([i['bottom'] for i in key_wrd_vt])
                            key_wrd['text'] = key_wrd['text'].strip() + '->'
                            key_wrd['top'] = top_min
                            key_wrd['bottom'] = bottom_max
                            key_wrd['text'] = key_wrd['text'].replace('|','').strip()
                            part2_key_right.append(key_wrd)
                        else:
                            key_wrd_right = key_wrd_right[0]
                            key_wrd_vlu = dict_concatenator(key_wrd, key_wrd_right)
                            key_wrd_vlu['text'] = key_wrd_vlu['text'].replace('|','').strip()
                            part2_key_right.append(key_wrd_vlu)

            final_down_vlu = []
            cnt = 0
            for key in part2_key_right:
                print('\nkey value is:', key)
                cnt += 1
                if cnt < len(part2_key_right):
                    next_vlu = part2_key_right[cnt]
                else:
                    next_vlu = copy.deepcopy(ending_clmn)
                    next_vlu['top'] = next_vlu['bottom']+1
                key_down = list(filter(lambda x: key['bottom']+0.2 < x['bottom'] <= next_vlu['top']+3, wrds_bf3_bel2_end))
                key_down = horiz_concat(key_down, spcl_cond = 'x', rng = 5)
                key_down = sorted(key_down, key = lambda i: i['x0'], reverse = False)
                # print('values after horiz concat:', [i['text'] for i in key_down])
                key_down = vertic_concat(key_down, rng = 2.4)
                key_down = sorted(key_down, key = lambda i: i['top'], reverse = False)
                down_dict = {}
                down_dict['Key'] = [key['text'].replace('->','').strip()]

                if len(page_check_values) > 0 and len(key_down) < 4:
                    for key_down_vlu3  in key_down:
                        print('key_down value3 is:',  joiner(key_down_vlu3['text']))
                        if joiner(key_down_vlu3['text']) in ['yes', 'no', 'xyes', 'xno']:
                            match_check = list(filter(lambda x: joiner(x.split()[0]) in joiner(key_down_vlu3['text']), page_check_values))
                            print('yes/no match check value is:', match_check)
    #                         if len(match_check) > 0:
    #                             print('key_down_vlu3:', key_down_vlu3, ':', key_down_vlu3['text'].strip()[1])
                            if key_down_vlu3['text'].strip()[0] != 'X':
                                key_down_vlu3['text'] = ' X '+ key_down_vlu3['text']
                                key_down = [key_down_vlu3]
                                break
                            else:
                                key_down = [key_down_vlu3]
    #                             print('cross_down_chk_ value is:', cross_down_chk_)
                                break
                if '->' in key['text'] and len(key_down) < 5:
                    srch_key = key['text'].replace('->','').strip()
                    down_vlus = part2_key_fullvalue[srch_key]
                    print('down_vlus are:', down_vlus)
                    print('page_check values are:', page_check_values)
                    if len(page_check_values) > 0:
                        for key_down_vlu4  in down_vlus:
                            print('key_down value4 is:', joiner(only_alpha(key_down_vlu4)))
                            match_check2 = list(filter(lambda x: SM(None, joiner(only_alpha(x)),joiner(only_alpha(key_down_vlu4))).ratio() > 0.9, page_check_values))
                            print('match_check2 value is:', match_check2)
                            if len(match_check2) > 0:
                                print('key_down_vlu4 value is:', key_down_vlu4, ':', key_down_vlu4.strip()[0])
                                if key_down_vlu4.strip()[0] != 'X':
                                    key_down_vlu4 = ' X '+ key_down_vlu4
                                down_vlus = [key_down_vlu4]
    #                             print('cross_down_chk_ value is:', cross_down_chk_)
                                break
                    print('down_vlus is:', down_vlus)

                    if len(down_vlus) == 1:
                        down_dict['Value_1'] = [down_vlus[0]]
                    elif len(down_vlus) > 1:
                        x_cond_down_chk = list(filter(lambda x:joiner(x)[0] == 'x', down_vlus))
                        if len(x_cond_down_chk) == 1:
                            down_dict['Value_1'] = [x_cond_down_chk[0]]
                        else:
                            for idx in range(len(down_vlus)):
                                down_dict[f'Value_{idx+1}'] = [down_vlus[idx]]
                elif len(key_down) > 0:    
                    if len(key_down) == 1:
                        key_down = key_down[0]
                        down_dict['Value_1'] = [key_down['text']]
                    elif len(key_down) >= 2:
                        down_sort = sorted(key_down, key = lambda i: i['x0'], reverse = False)
                        down_frst_ln = list(filter(lambda x: key_down[0]['top'] < x['y_mean'] < key_down[0]['bottom'], down_sort))
                        down_lastbut_1= list(filter(lambda x: key_down[-2]['top'] < x['y_mean'] < key_down[-2]['bottom'], down_sort))
                        for idx_c in range(len(down_sort)):
                            if down_sort[idx_c]['x1'] - down_sort[idx_c]['x0'] < 105 and len(joiner(down_sort[idx_c]['text'])) > 0:
                                # print('reference index is:', down_sort[idx_c])
                                sort_idx = idx_c
                                break
                            else:
                                sort_idx = 0
                                continue
                        down_cnd_chk = list(filter(lambda x: down_sort[sort_idx]['top'] < x['y_mean'] < down_sort[sort_idx]['bottom'], down_sort))
        #                 if len(down_cnd_chk) == 1 and len(down_frst_ln) ==1:
                        if len(down_frst_ln) == 1 and len(down_lastbut_1) == 1:
                            down_dict['Value_1'] = [' '.join([i['text'] for i in key_down])]
                        else:
                            key_dict = copy.deepcopy(key_down)
        #                     bng_key_srch = list(filter(lambda x: joiner(x['text']) == 'beginning', key_dict))
                            value_range = []
        #                     if len(bng_key_srch) > 0:
        #                         bng_key_srch = bng_key_srch[0]
        #                         prft_key_srch = list(filter(lambda x: joiner(x['text']) == 'profit' and x['x1'] < bng_key_srch['x0'], key_dict))
        #                         end_key_srch = list(filter(lambda x: joiner(x['text']) == 'ending' and bng_key_srch['top'] < x['y_mean'] < bng_key_srch['bottom'], key_dict))
        #                         if len(prft_key_srch) > 0 and len(end_key_srch) > 0:
        #                             prft_key_srch = prft_key_srch[0]
        #                             end_key_srch = end_key_srch[0]
        #                             value_range.append([{'x0':prft_key_srch['x0'],'x1':prft_key_srch['x1']},
        #                                                 {'x0':bng_key_srch['x0'],'x1':bng_key_srch['x1']},
        #                                                 {'x0':end_key_srch['x0'],'x1':end_key_srch['x1']}])
                            # print('else cond values:', [i['text'] for i in key_dict])

                            for vlu in down_cnd_chk:
                                value_range.append({'x0':int(vlu['x0'])-1, 'x1':int(vlu['x1'])+1})
                            not_matched = [i for i in key_dict if word_range_not_matched(i, value_range)]   
                            if len(not_matched) > 0:
    #                                 print('nm_values are:', not_matched, '\n','--'*85)
                                for nm in not_matched:     

                                    if word_range_not_matched(nm, value_range):
                                        value_range.append({'x0':nm['x0'], 'x1':nm['x1']})
                                    else:
                                        continue
                            value_range = sorted(value_range, key = lambda i: i['x0'], reverse = False)

        #                     print('value_range_lst:', value_range, '\n','--'*85)
                            for idx in range(len(value_range)):
                                down_dict[f'Value_{idx+1}'] = []
                            vlus = []
                            matched = [i for i in down_dict if i != 'Key']
                            for dwn_vlu in key_dict:
    #                                 print('down_value is:', dwn_vlu['text'])
                                if len(dwn_vlu['text'].strip()) == 0 or joiner(dwn_vlu['text']) == '▶':
    #                                     print('second chk:', dwn_vlu)
                                    continue
                                chk_ = list(filter(lambda x: x['x0']-2 <= dwn_vlu['x0'] <= x['x1']+2.5, value_range))
                                if len(chk_) > 0:
                                    idx_chk = value_range.index(chk_[0])
                                else:
                                    chk_ = list(filter(lambda x: x['x0']-2 <= dwn_vlu['x_mean'] <= x['x1']+2.5, value_range))
                                    idx_chk = value_range.index(chk_[0])
                                    if len(chk_) == 0:
                                        chk_ = list(filter(lambda x: x['x0']-2 <= dwn_vlu['x1'] <= x['x1']+2.5, value_range))
                                        idx_chk = value_range.index(chk_[0])
                                if len(vlus) == 0:
                                    down_dict[f'Value_{idx_chk+1}'].append(dwn_vlu['text'])
                                    matched.pop(matched.index(f'Value_{idx_chk+1}'))
                                    vlus.append(dwn_vlu)
                                else:
                                    diff_val = abs(vlus[len(vlus) -1]['bottom'] - dwn_vlu['bottom'])
                                    if diff_val < 3:
                                        # print('--'*85)
                                        # print('prev vlu is:', vlus[len(vlus) -1]['text'])
                                        # print('present vlu is:', dwn_vlu['text'])
                                        # print('--'*85)
                                        if f'Value_{idx_chk+1}' in matched:
                                            down_dict[f'Value_{idx_chk+1}'].append(dwn_vlu['text'])
                                            matched.pop(matched.index(f'Value_{idx_chk+1}'))
                                        else:
                                            chk_ = list(filter(lambda x: x['x0']-2 <= dwn_vlu['x_mean'] <= x['x1']+2.5, value_range))
                                            idx_chk = value_range.index(chk_[0])
                                            if f'Value_{idx_chk+1}' in matched:
                                                down_dict[f'Value_{idx_chk+1}'].append(dwn_vlu['text'])
                                                matched.pop(matched.index(f'Value_{idx_chk+1}'))
                                            else:
                                                down_dict[f'Value_{idx_chk+1}'][-1] += f" {dwn_vlu['text']}"
                                        vlus.append(dwn_vlu)
                                    else:
                                        [down_dict[i].append(' ') for i in matched]
                                        matched = [i for i in down_dict if i != 'Key']
                                        down_dict[f'Value_{idx_chk+1}'].append(dwn_vlu['text'])
                                        matched.pop(matched.index(f'Value_{idx_chk+1}'))
                                        vlus.append(dwn_vlu)
        #                     print('\n down_dict value is:', down_dict)
                            final_down_vlu.append(down_dict)
                else:
                    down_dict['Value_1'] = ['-']
                final_down_vlu.append(down_dict)
                final_down_vlu_df = []
                for vlu_dn in final_down_vlu:
                    final_down_vlu_df.append(pd.DataFrame.from_dict(vlu_dn,orient='index').T.fillna(' '))
                part2_final_df_ = concatenator(final_down_vlu_df).drop_duplicates().reset_index(drop=True)
                start_idx = 0
                stop_idx = 0
                idx_cnt = -1
                for df_vlu in part2_final_df_['Key']:
                    idx_cnt += 1
                    if len(df_vlu.strip()) == 0:
                        continue
                    elif df_vlu.split()[0].strip() == 'L':
                        start_idx = idx_cnt
                    elif df_vlu.split()[0].strip() == 'M':
                        stop_idx = idx_cnt
                        break

                headers_L = part2_final_df_[start_idx:stop_idx]['Value_1']
                sub_table_result = []
                for l_vlu in headers_L:
                    if len(sub_table_result) == 0:
                        sub_table_result.append(part2_final_df_[:start_idx+1])
                        header_l_spill = part2_final_df_['Key'][start_idx].split()
                        header_vlu = f'ITEM {header_l_spill[0]}. {" ".join(header_l_spill[1:])}'
                        print('header_vlu is:', header_vlu)
                        header_mtch_fin = value_extractor(pdf_pages, header_vlu, nxt_vlu, form)
                        if 'Warning' in header_mtch_fin.columns.tolist():
                            continue
                        else:
                            header_mtch_fin.columns = ['Value_1', 'Value_2']
                            sub_table_result.append(header_mtch_fin)
    #                         sub_table_result.append(pd.DataFrame([part2_final_df_.loc[start_idx]]))
                    start_idx += 1
                    header_vlu = f'{l_vlu}, ITEM L, CODE L'
                    header_mtch_fin = value_extractor(pdf_pages, header_vlu, nxt_vlu, form)
        #             print('header_mtch_fin value is:\n', header_mtch_fin)
                    if 'Warning' in header_mtch_fin.columns.tolist():
                        sub_table_result.append(pd.DataFrame([part2_final_df_.loc[start_idx]]))
                        continue
                    else:
                        header_mtch_fin.columns = ['Value_1', 'Value_2']
                        sub_table_result.append(header_mtch_fin)
                        sub_table_result.append(pd.DataFrame([part2_final_df_.loc[start_idx]]))
                sub_table_result.append(part2_final_df_[start_idx+1:])
                if len(sub_table_result) > 0:
                    part2_final_df = concatenator(sub_table_result)
                else:
                    part2_final_df = part2_final_df_
                part2_final_df.drop_duplicates().reset_index(drop=True)
                part2_final_df.fillna(' ', inplace = True)
            return part2_final_df

        def word_range_not_matched(wrd, ranges):
            ranges_ = copy.deepcopy(ranges)
            wrd_ = copy.deepcopy(wrd)
    #             print('ranges value is:', ranges)
    #             print('word_range wrd is:', wrd)
            match = list(filter(lambda x: int(wrd_['x0']) in range(int(x['x0'])-1, int(x['x1'])+1)
                                or int(wrd_['x_mean']) in range(int(x['x0'])-1, int(x['x1'])+1)
                                or int(wrd_['x1']) in range(int(x['x0'])-1, int(x['x1'])+1), ranges_))
    #             print('word range match value is:', match)
            if len(match)>0:
                return False
            else:
                return True

        # def range_creator(wrd, ranges):
        #     ranges_ = copy.deepcopy(ranges)
        #     wrd_ = copy.deepcopy(wrd)
        #     range_lst = []

        #     match = list(filter(lambda x: ))

        def text_cleaner_part2(wrds_):
            wrds = copy.deepcopy(wrds_)
            wrds_lp = []
            unclean = []
            for wrd in wrds:
                    if ('}}}' not in wrd['text']) and \
            ('~~' not in wrd['text']) and ('---' not in wrd['text']) \
            and ('===' not in wrd['text']) and ('.' != wrd['text'].strip()):
                        if '\xa0' in wrd['text'] or '\x7f' in wrd['text'] or '(cid:' in wrd['text']:
                            new_vlu = copy.deepcopy(wrd['text'].replace(wrd['text'][wrd['text'].find('('):wrd['text'].find(')',wrd['text'].find('('))+1],''))
                            wrd['text'] = new_vlu.replace('\xa0','').replace('\x7f','').replace('(cid:160)','').strip()
                        wrds_lp.append(wrd)
                    else:
                        if '~~' in wrd['text'] and len(wrd['text'].replace('~','').strip()) > 0:
                            wrd['text'] = wrd['text'].replace('~','').replace('(cid:160)','').strip()
                            wrds_lp.append(wrd)
                        else:
                            unclean.append(wrd)

            return wrds_lp

        def vertic_concat(wrds_, rng = 2):
            wrds = copy.deepcopy(wrds_)
            result = []
            for wrd in wrds:
                if len(result) == 0:
                    result.append(wrd)
                else:
                    diff = abs(result[len(result) - 1]['bottom'] - wrd['top'])
                    num_check_tp= [i for i in result[len(result) - 1]['text'] if i.isnumeric()]
                    num_check_btm = [i for i in wrd['text'] if i.isnumeric()]
                    if diff < rng and len(num_check_btm) < 2 and len(num_check_tp) < 2 and len(result[len(result) - 1]['text'].strip()) > 3:
                        vlu_ = dict_concatenator(result[len(result) - 1], wrd)
                        result.pop(-1)
                        result.append(vlu_)
                    else:
                        result.append(wrd)
            return result

        def concatenator(dfs, dummy = None):
            final_df = []
            space_ = pd.DataFrame({'Key':[' ']})

            if dummy == None:
                for df in dfs:
                    final_df.append(pd.DataFrame(df).reset_index(drop = True))
            else:
                cnt = 0
                for df in dfs:
                    final_df.append(pd.DataFrame(df).reset_index(drop = True))
                    if cnt + 1 < len(dfs):
                        final_df.append(space_)

            return pd.concat(final_df)

        # Form
        ######################################
        ######################################

        # Form
        ######################################
        ######################################






        def tablemerge(tel):
            tb_df = pd.DataFrame(tel[0])
            header_df = copy.deepcopy(tel[0][:4])
            footer_df = copy.deepcopy(tel[0][-2:])
            for i in header_df:
                i.insert(1," ")
            for i in footer_df:
                i.insert(1," ")
            header_df = pd.DataFrame(header_df)
            footer_df = pd.DataFrame(footer_df)
            fc = tb_df[4:-2][0]
            fc_list = fc.to_list()
            final_list = []
            for i in fc_list:
                split_value = i.split(' ')[-1]
                re_v = re.findall(r'\d+(?:[,.]\d+)*', split_value)
                if len(re_v) > 0:
                    if len(i.split(' ')) > 1:
                        process_str = [' '.join(i.split(' ')[:-1]), i.split(' ')[-1]]
                    else:
                        process_str = [' ', i.split(' ')[-1]]
                    final_list.append(process_str)

                else:
                    re_v = re.findall(r'\d+(?:[,.]\d+)*', i)
                    if len(re_v) > 0:
                        process_str = [i, re_v[0]]
                        final_list.append(process_str)
                    else:
                        process_str = [i, ' ']
                        final_list.append(process_str)
            fc_df = pd.DataFrame(final_list)
            othercolumns_data = tb_df.iloc[4:-2 , 1:].reset_index(drop=True)
            f_v = pd.concat([fc_df, othercolumns_data],axis=1)
            f_v.columns = range(f_v.columns.size)
            final_df_res = header_df.append(f_v)
            final_df_res = final_df_res.append(footer_df)
            final_df_res = final_df_res.reset_index(drop=True)
            return final_df_res
        def header_kv(header, text_h, pg_index):
            text = list(text_h)
            print('input header value is:', header)
            page_number = pg_index + 1
            text_process = handle_multipleline_kv(text)
            words_list = handle_multiple_row2_kv(text_process)
            sample_header = header
            print('actual header value is:', cleanse_key(header))
            if '\n' in header:
                cnt_h = 0
                _headers = sample_header.split('\n')
                headers_start = _headers[0]
                headers_end = _headers[-1]
                _headers_start = list(filter(lambda x: cleanse_key(headers_start) in cleanse_key(x['text']), words_list))
                print('_headers_start value is/are:',_headers_start)
                _headers_start = [i for i in _headers_start if cleanse_key(headers_start) == cleanse_key(i['text'])]
                print('_headers_start match value is:',_headers_start)
                _headers_end = list(filter(lambda x: cleanse_key(headers_end) in cleanse_key(x['text']), words_list))
                print('_headers_end value is/are:',_headers_end)
                _headers_end = [i for i in _headers_end if cleanse_key(headers_end) == cleanse_key(i['text'])]
                print('_headers_end match value is:',_headers_end)
                _headers_ms = multi_single(_headers_start, _headers_end)
                print('_headers_ms multi single value is:', _headers_ms)
                if len(_headers_start) == 0:
                        cnt_h = 1
                        headers_start_new = headers_start + _headers[cnt_h]
                        _headers_start_new = list(filter(lambda x: cleanse_key(headers_start_new) in cleanse_key(x['text']), words_list))
                        print('_headers_start_new value is/are:',_headers_start_new)
                        _headers_start = [i for i in _headers_start_new if cleanse_key(headers_start_new) == cleanse_key(i['text'])]
                        print('_headers_start new match value is:',_headers_start)
                        _headers_ms = _headers_start

                _headers_ms = multi_header_values_kv(_headers_ms)
                print('_headers_ms processed through mulit_header_values_kv', _headers_ms)
                headers = _headers_ms
                if cnt_h == 0:
                    new_column = headers_start+' '+headers_end
                elif cnt_h == 1:
                    new_column = headers_start_new
                if headers:
                    headers_sm_value = SM(None, cleanse_key(new_column), cleanse_key(headers[0]['text'])).ratio()
                    print('headers_sm_value is:', headers_sm_value)
                    if headers_sm_value > 0.85:
                        headers = headers[0]
                        print('final header and page number with /n is:',page_number, headers["text"])
                    else:
                        return
                if len(headers) == 0:
                    return


            else:
                c_hd = 0
                _headers1 = list(filter(lambda x: cleanse_key(header) in cleanse_key(x['text']), words_list))
                print('_headers value is:',page_number, _headers1)
                _headers = [i for i in _headers1 if cleanse_key(header) == cleanse_key(i['text'])]
                print("headers value is:",page_number, _headers)
                if len(_headers) == 0:
                    c_hd = 1
                    _headers1 = list(filter(lambda x: cleanse_key(header+'continued') in cleanse_key(x['text']), words_list))
                    print('_headersspecial value is:',page_number, _headers1)
                    _headers = [i for i in _headers1 if cleanse_key(header+'continued') == cleanse_key(i['text'])]
                    print("headersspecial value is:",page_number, _headers)
                if len(_headers) == 0:
                    c_hd = 0
                    # split, search and match
                    print('split, search and match condition started')
                    header_split = sample_header.split(' ')
                    print('header_split value is:', header_split)
                    if len(header_split) == 2:
                        split_1 = header_split[0]
                        split_2 = header_split[-1]
                        print('split1 and 2 values are:', split_1, split_2)
                        header_start = list(filter(lambda x: cleanse_key(split_1) in cleanse_key(x['text']), words_list))
                        print('header_start search values are', header_start)
                        header_start = [i for i in header_start if cleanse_key(i['text']) == cleanse_key(split_1)]
                        if len(header_start) > 1:
                            header_start = [header_start[0]]
                        elif len(header_start) == 0:
                            return
                        else:
                            header_start = header_start
                        print('header_start match values are', header_start)
                        header_end = list(filter(lambda x: cleanse_key(split_2) in cleanse_key(x['text']), words_list))
                        print('header_end search values are', header_end)
                        header_end = [i for i in header_end if cleanse_key(i['text']) == cleanse_key(split_2)]
                        if len(header_end) > 1:
                            header_end = [header_end[0]]
                        elif len(header_end) == 0:
                            return
                        else:
                            header_end = header_end
                        print('header_end match values are', header_end)
                        _headers = multi_single(header_start, header_end)
                        print('headers split match value is:', _headers)
                if len(_headers) == 0:
                    return
                if len(_headers)>1:
                    headers_start = [i['x1'] for i in _headers]
                    headersmin = np.min(headers_start)
                    _headers = [i for i in _headers if i['x0']<=headersmin]
                    print(f'headers matched after split and min start: {page_number}', _headers)
                    if len(_headers)>1:
                        sample = _headers
                        header_top = [i['top'] for i in _headers]
                        headermin = np.min(header_top)
                        _headers = [i for i in sample if i['top']==headermin]
                        print(f'headers matched after min start and min top:{page_number}',_headers)

                if _headers:
                    if c_hd == 0:
                        headers_sm_value = SM(None, cleanse_key(header), cleanse_key(_headers[0]['text'])).ratio()
                        print('Actual header and selected header is:', header, _headers)
                    if c_hd == 1:
                        headers_sm_value = SM(None, cleanse_key(header+'continued'), cleanse_key(_headers[0]['text'])).ratio()
                        print('Actual header and selected header is:', header, _headers)
                    print('headers Sequence Matcher value is:', headers_sm_value)
                    if headers_sm_value > 0.85:
                        headers = _headers[0]
                    else:
                        return
                else:
                    return
                if len(headers) == 1:
                    headers = headers[0]
                else:
                    headers = headers
                print(f'headers value and page_number is:\n {headers}, page_number:{page_number}')

            return headers

        def ending_key_kv(ending_key, text_k, he, pg_index):
    #     print('text_kv values are:', text_k)
            wrd_pr = pdf_ob.pages[pg_index].extract_words(x_tolerance = 3, y_tolerance = 3, keep_blank_chars = True)
            multi_keys_condition = 0
            text = list(text_k)
            global k_down_2
            page_number = pg_index + 1
            x_header = he
            text_process = handle_multipleline_kv(text)
            words_list = handle_multiple_row2_kv(text_process)
            sample_endk = ending_key
            print('Actual ending_key value is:', cleanse_key(ending_key))
            if '\n' in ending_key:
                k_down_2 = 1

                _endk = sample_endk.split('\n')
                if len(_endk) <3:
                    cnt = 0
                    endk_start = _endk[cnt]
                    endk_end = _endk[-1]
                    _endk_start = list(filter(lambda x: cleanse_key(endk_start) in cleanse_key(x['text']), words_list))
                    print('_endk_start value is/are:',_endk_start)
                    _endk_start = [i for i in _endk_start if cleanse_key(endk_start) == cleanse_key(i['text'])]
                    print('_endk_start match value is:',_endk_start)
                    if len(_endk_start) == 0:
                        print('endk start value is zero')
                        print('actual ending key is', sample_endk)
                        print('split start value is:', endk_start)
                        print('split end value is:', endk_end)

                        _endk_start = list(filter(lambda x: cleanse_key(endk_start) in cleanse_key(x['text']), wrd_pr))
                        print('_endk_start value is/are:',_endk_start)
                        _endk_start = [i for i in _endk_start if cleanse_key(endk_start) == cleanse_key(i['text'])]
                        print('_endk_start match value is:',_endk_start)

                    _endk_end = list(filter(lambda x: cleanse_key(endk_end) in cleanse_key(x['text']), words_list))
                    print('_endk_end value is/are:',_endk_end)
                    _endk_end = [i for i in _endk_end if cleanse_key(endk_end) == cleanse_key(i['text'])]
                    print('_endk_end match value is:',_endk_end)
                    _endk_ms = multi_single(_endk_start, _endk_end)
                    print('_endk_ms multi single value is:', _endk_ms)

                    if len(_endk_start) == 0:
                        cnt = 1
                        endk_start_new = endk_start + _endk[cnt]
                        _endk_start_new = list(filter(lambda x: cleanse_key(endk_start_new) in cleanse_key(x['text']), words_list))
                        print('_endk_start_new value is/are:',_endk_start_new)
                        _endk_start = [i for i in _endk_start_new if cleanse_key(endk_start_new) == cleanse_key(i['text'])]
                        print('_endk_start new match value is:',_endk_start)
                        _endk_ms = _endk_start

                    if len(_endk_ms) >= 1:
                        end_k = _endk_ms
                        print('endk value for _endk_ms == 1:', _endk_ms)
        #             elif len(_endk_ms) > 1:
        #                 endk_d = _endk_ms[0]
        #                 endk = [endk_d]
                    else:
                        return
                elif len(_endk) > 2:
                    cnt = 2
                    end_k = multi_line_rows(sample_endk, words_list)
                    if end_k == None:
                        return
                    if len(end_k) > 5:
                        end_k = [end_k]
                    print('_endk1 value is:', end_k)
        #             if len(endk_1) == 1:
        #                 endk = _endk1
        #             if len(_endk1) > 1:
        #                 endk = [_endk1[0]]
        #             print('endk value is:', endk)

                if len(end_k) == 1:
                    if cnt == 0:
                        new_key = endk_start+' '+endk_end
                        endk_sm_value = SM(None, cleanse_key(new_key), cleanse_key(end_k[0]['text'])).ratio()
                        if endk_sm_value > 0.85:
                            endk = end_k[0]
                            print(f'ending_key value and page_number is:\n {end_k[0]}, page_number:{page_number}')
                        else:
                            return
                    elif cnt == 1:
                        new_key = endk_start_new+ ' '+endk_end
                        new_key2 = endk_start_new
                        endk_sm_value2 = SM(None, cleanse_key(new_key2), cleanse_key(end_k[0]['text'])).ratio()
                        endk_sm_value = SM(None, cleanse_key(new_key), cleanse_key(end_k[0]['text'])).ratio()
                        print('endk_sm_value is:', endk_sm_value)
                        if endk_sm_value > 0.85:
                            endk = end_k[0]
                            print(f'ending_key value and page_number is:\n {end_k[0]}, page_number:{page_number}')
                        elif endk_sm_value <0.85 and endk_sm_value2 > 0.85:
                            endk = end_k[0]
                        else:
                            return
                    else:
                        endk_sm_value = SM(None, cleanse_key(ending_key), cleanse_key(end_k[0]['text'])).ratio()
                        print('Actual and selected ending_key are:', ending_key, end_k)
                        print('ending_key_sm_value is:', endk_sm_value)
                        if endk_sm_value > 0.85:
                            endk = end_k[0]
                            print(f'ending_key value and page_number is:\n {end_k[0]}, page_number:{page_number}')
                        else:
                            return

                elif len(end_k) > 1:
                    multi_keys_condition = 1
                    endk_multi =  []
                    end_k = multi_key_extractor(end_k)
                    for idx in range(len(end_k)):
                        if cnt == 0:
                            new_key = endk_start+' '+endk_end
                            endk_sm_value = SM(None, cleanse_key(new_key), cleanse_key(end_k[idx]['text'])).ratio()
                            if endk_sm_value > 0.85:
                                endk_multi.append(end_k[idx])
                                print(f'ending_key value and page_number is:\n {end_k[idx]}, page_number:{page_number}')
                            else:
                                continue
                        elif cnt == 1:
                            new_key = endk_start_new+ ' '+endk_end
                            new_key2 = endk_start_new
                            endk_sm_value2 = SM(None, cleanse_key(new_key2), cleanse_key(end_k[idx]['text'])).ratio()
                            endk_sm_value = SM(None, cleanse_key(new_key), cleanse_key(end_k[idx]['text'])).ratio()
                            print('endk_sm_value is:', endk_sm_value)
                            if endk_sm_value > 0.85:
                                endk_multi.append(end_k[idx])
                                print(f'ending_key value and page_number is:\n {end_k}, page_number:{page_number}')
                            elif endk_sm_value <0.85 and endk_sm_value2 > 0.85:
                                endk_multi.append(end_k[idx])
                            else:
                                continue



            else:
                k_down_2 = 0
                _endk = list(filter(lambda x:cleanse_key(ending_key) in cleanse_key(x['text']),words_list))
                _endk = [i for i in _endk if SM(None, cleanse_key(ending_key), cleanse_key(i['text'])).ratio() > 0.9]
                print('_endk matched value is:', _endk)
                if len(_endk) == 0:
                    _endk = list(filter(lambda x:cleanse_key(ending_key) in cleanse_key(x['text']), text))
                    _endk = [i for i in _endk if cleanse_key(ending_key) == cleanse_key(i['text'])]
                    print(f'_endk matched value after deprocessing in page:{page_number}', _endk)
                endk = _endk
                endk_multi = []
                if len(endk) > 1:
                    endk = [i for i in _endk if i['top'] > x_header['top']+1]
                    print('endk value is:', endk)
                if len(endk) > 1:
                    multi_key = multi_key_extractor(endk)
                    for endk_m in multi_key:
                        endk_m = [endk_m]
                        if len(endk_m) == 1:
                            endk_sm_value = SM(None, cleanse_key(ending_key), cleanse_key(endk_m[0]['text'])).ratio()
                            print('Actual and selected ending_key are:', ending_key, endk_m)
                            print('ending_key_sm_value is:', endk_sm_value)
                            if endk_sm_value > 0.85:
                                endk_m = endk_m
                        if len(endk_m) == 0:
                            return
                        if len(endk_m) == 1:
                            endk_m = endk_m[0]
                        else:
                            endk_m = endk_m
                        print(f'ending_key value and page_number is:\n {endk}, page_number:{page_number}')
                        multi_keys_condition = 1
                        endk_multi.append(endk_m)

                if len(endk) == 1:
                    endk_sm_value = SM(None, cleanse_key(ending_key), cleanse_key(endk[0]['text'])).ratio()
                    print('Actual and selected ending_key are:', ending_key, endk)
                    print('ending_key_sm_value is:', endk_sm_value)
                    if endk_sm_value > 0.85:
                        endk = endk[0]
                if len(endk) == 0:
                    return
                if len(endk) == 1:
                    endk = endk[0]
                else:
                    endk = endk
                print(f'ending_key value and page_number is:\n {endk}, page_number:{page_number}')
            if multi_keys_condition == 1:
                print(f'ending_key multi value and page_number is:\n {endk_multi}, page_number:{page_number}')
                return endk_multi
            else:
                return [endk]

        def ending_column_kv(ending_column, text_c, he, enkv, pg_index):
            text = list(text_c)
            page_number = pg_index + 1
            text_process = handle_multipleline_kv(text)
            words_list1 = handle_multiple_row2_kv(text_process)
            words_list = list(words_list1)
            sample_endc = ending_column
            print('Actual ending_column is:',ending_column)
            print('after cleansing, value is:', cleanse_key(ending_column))
            cnt_c = 0
            if '\n' in ending_column:
                _endc = sample_endc.split('\n')
                endc_start = _endc[0]
                endc_end = _endc[-1]
                print('endc_start and endc_end values are:', endc_start, endc_end)
                _endc_start = list(filter(lambda x: cleanse_key(endc_start) in cleanse_key(x['text']), words_list))
                print('_endc_start value is/are:',_endc_start)
                _endc_start = [i for i in _endc_start if cleanse_key(endc_start) == cleanse_key(i['text'])]
                print('_endc_start match value is:',_endc_start)
                _endc_end = list(filter(lambda x: cleanse_key(endc_end) in cleanse_key(x['text']), words_list))
                print('_endc_end value is/are:',_endc_end)
                _endc_end = [i for i in _endc_end if cleanse_key(endc_end) == cleanse_key(i['text'])]
                print('_endc_end match value is:',_endc_end)
                _endc_ms = multi_single(_endc_start, _endc_end)
                print('_endc_ms multi single value is:', _endc_ms)
            #     if len(_endc_ms) > 1:
                if len(_endc_start) == 0:
                    cnt_c = 1
                    endc_start_new = endc_start + _endc[cnt_c]
                    _endc_start_new = list(filter(lambda x: cleanse_key(endc_start_new) in cleanse_key(x['text']), words_list))
                    print('_endc_start_new value is/are:',_endc_start_new)
                    _endc_start = [i for i in _endc_start_new if cleanse_key(endc_start_new) == cleanse_key(i['text'])]
                    print('_endc_start new match value is:',_endc_start)
                    _endc_ms = _endc_start
                print('_endc_ms value is:', _endc_ms)
                _endc_ms = multi_column_values_kv(_endc_ms,he, enkv)
                print('_endc_ms processed through mulit_column_values_kv', _endc_ms)
                endc = _endc_ms
                print('endc value is:', endc)
                if endc == None:
                    return
                if len(endc) == 0:
                    pass
                if len(endc):
                    if cnt_c == 0:
                        new_key_c = endc_start+' '+endc_end
                        print('new_key_c value is:', new_key_c)
                        endc_sm_value = SM(None, cleanse_key(new_key_c), cleanse_key(endc[0]['text'])).ratio()
                        if endc_sm_value > 0.85:
                            endc = endc[0]
                            print(f'ending_key value and page_number is:\n {endc}, page_number:{page_number}')
                        else:
                            pass
                    elif cnt_c == 1:
                        new_key_c = endc_start_new
                        endc_sm_value = SM(None, cleanse_key(new_key_c), cleanse_key(endc[0]['text'])).ratio()
                        print('endc_sm_value is:', endc_sm_value)
                        if endc_sm_value > 0.85:
                            endc = endc[0]
                            print(f'ending_column value and page_number is:\n {endc}, page_number:{page_number}')
                        else:
                            pass
            else:
                _endc = list(filter(lambda x:cleanse_key(ending_column) in cleanse_key(x['text']),words_list))
                print('_endc values in page number is:', page_number, _endc)
                _endc = [i for i in _endc if cleanse_key(ending_column) == cleanse_key(i['text'])]
                _endc = [i for i in _endc if i['top'] < enkv['bottom']]
                #
                print('_endc values matched in page number:', page_number, _endc)
                _endc_ms = multi_column_values_kv(_endc,he, enkv)
                print('_endc_ms processed through multi_column_values_kv', _endc_ms)
                endc = _endc_ms
                if len(endc) > 1:
                    endc = multi_column_values_kv(endc, he, enkv)
                print('endc value in pagenumber:', page_number, endc)
                if endc:
                    endc_sm_value = SM(None, cleanse_key(ending_column), cleanse_key(endc[0]['text'])).ratio()
                    print('endc_sm_value is:', endc_sm_value)
                    if endc_sm_value > 0.85:
                        endc = endc[0]
                        print(f'ending_c value and page_number is:\n {endc}, page_number:{page_number}')
                    else:
                        pass

            if len(endc) == 0:
                return
            if len(endc) == 1:
                endc = endc[0]
            else:
                endc = endc
            return endc

        def multi_single(row_1, row_2):
            temp = []
            row1 = list(row_1)
            row2 = list(row_2)
            sample_row1 = row_1
            print('multi single input:', row1, row2)
            if len(row1) == len(row2):
                for row_s,row_e in zip(row1, row2):
                    diff_val_top = abs(row_s["bottom"] - row_e['top'])
                    diff_val_bottom = abs(row_s['bottom'] - row_e['bottom'])
                    print('diff_val_top and bottom', diff_val_top, diff_val_bottom)
                    if diff_val_top < 16 or 0.3> diff_val_bottom >= 0:
                        row_s['text'] = row_s['text']+' '+row_e['text']
                        row_s['x0'] = sorted(list(set([row_s['x0'], row_e['x0']])))[0]
                        row_s['x1'] = sorted(list(set([row_s['x1'], row_e['x1']])))[-1]
                        row_s['top'] = sorted(list(set([row_s['top'], row_e['top']])))[0]
                        row_s['bottom'] = sorted(list(set([row_s['bottom'], row_e['bottom']])))[-1]
                        row_s['doctop'] = sorted(list(set([row_s['doctop'], row_e['doctop']])))[-1]
                        temp.append(row_s)
            else:
                for i in range(len(row1)):
                    for j in range(len(row2)):
                        row1[i] = sample_row1[i]
                        diff_val_top = abs(row1[i]["bottom"] - row2[j]['top'])
                        if diff_val_top < 16:
                            row1[i]['text'] = row1[i]['text']+' '+row2[j]['text']
                            row1[i]['x0'] = sorted(list(set([row1[i]['x0'], row2[j]['x0']])))[0]
                            row1[i]['x1'] = sorted(list(set([row1[i]['x1'], row2[j]['x1']])))[-1]
                            row1[i]['top'] = sorted(list(set([row1[i]['top'], row2[j]['top']])))[0]
                            row1[i]['bottom'] = sorted(list(set([row1[i]['bottom'], row2[j]['bottom']])))[-1]
                            row1[i]['doctop'] = sorted(list(set([row1[i]['doctop'], row2[j]['doctop']])))[-1]
                            temp.append(row1[i])
            print('temp value is:', temp)
            return temp    

        def handle_multipleline_kv(wordslist):
            words_temp = []
            for i in range(len(wordslist)):
                if len(words_temp) == 0:    
                    words_temp.append(wordslist[i])
                else:
                    diff_val = abs(words_temp[len(words_temp)-1]['x1'] - wordslist[i]['x0'])
                    if diff_val >=0 and diff_val < 7.2:
                        words_temp[len(words_temp)-1]['text'] = words_temp[len(words_temp)-1]['text']+wordslist[i]['text']
                        words_temp[len(words_temp)-1]['x0'] = sorted(list(set([words_temp[len(words_temp)-1]['x0'], wordslist[i]['x0']])))[0]
                        words_temp[len(words_temp)-1]['x1'] = sorted(list(set([words_temp[len(words_temp)-1]['x1'], wordslist[i]['x1']])))[-1]
                        words_temp[len(words_temp)-1]['top'] = sorted(list(set([words_temp[len(words_temp)-1]['top'], wordslist[i]['top']])))[0]
                        words_temp[len(words_temp)-1]['bottom'] = sorted(list(set([words_temp[len(words_temp)-1]['bottom'], wordslist[i]['bottom']])))[-1]
                        words_temp[len(words_temp)-1]['doctop'] = sorted(list(set([words_temp[len(words_temp)-1]['doctop'], wordslist[i]['doctop']])))[-1]
                    else:
                        words_temp.append(wordslist[i])
            return words_temp

        def handle_multiple_row2_kv(wordslist):
            words_temp = []                        
            for i in range(len(wordslist)):
                if len(words_temp) == 0:    
                    words_temp.append(wordslist[i])
                else:
                    diff_val = abs(words_temp[len(words_temp)-1]['x1'] - wordslist[i]['x0']) 
                    diff_val_doctop = abs(words_temp[len(words_temp)-1]['doctop'] - wordslist[i]['doctop']) 
                    if diff_val <=0.5 and diff_val_doctop<=11:
                        words_temp[len(words_temp)-1]['text'] = words_temp[len(words_temp)-1]['text']+' '+wordslist[i]['text']
                        words_temp[len(words_temp)-1]['x0'] = sorted(list(set([words_temp[len(words_temp)-1]['x0'], wordslist[i]['x0']])))[0]
                        words_temp[len(words_temp)-1]['x1'] = sorted(list(set([words_temp[len(words_temp)-1]['x1'], wordslist[i]['x1']])))[-1]
                        words_temp[len(words_temp)-1]['top'] = sorted(list(set([words_temp[len(words_temp)-1]['top'], wordslist[i]['top']])))[0]
                        words_temp[len(words_temp)-1]['doctop'] = sorted(list(set([words_temp[len(words_temp)-1]['doctop'], wordslist[i]['doctop']])))[-1]
                    else:
                        words_temp.append(wordslist[i]) 
            return words_temp

        def cleanse_key(key):
            key = str(key)
            key = key.lower()
            key = key.replace('d_', '')
            key = key.replace("'", "")
            key = key.replace('(continued)', '')
            key = key.replace(' ', '')
            if '  ' in key:
                key = ' '.join(key.split())
            return key


        def multi_header_values_kv(m_values):
            m_ = []
            m_sample = []
            if len(m_values) == 1:
                m_.append(m_values[0])
            elif len(m_values) > 1:
                header_top = [i['top'] for i in m_values]
                header_x0 = [i['x0'] for i in m_values]
                x0_min = np.min(header_x0)
                top_min = np.min(header_top)
                for j in m_values:
                    if j['top'] == top_min:
                        m_sample.append(j)
                if len(m_sample) == 1:
                    m_.append(m_sample[0])
                if len(m_sample) > 1:
                    m_sample = [i for i in m_sample if i['x0'] == x0_min]
                    m_.append(m_sample[0])
            return m_
        def multi_key_values_kv(m_endk_values, x_header):
            m_endk = []
            m_endk_sample = []
            m_endk_sample2 = []
            m_endk_sample3 = []
            for i in m_endk_values:
                if i['top'] >x_header['top'] and i['x0'] >= x_header['x0']:
                    print('below header values are:', i)
                    m_endk_sample.append(i)
            print('no of matching values:', m_endk_sample)
            if len(m_endk_sample) == 1:
                print('endk_sample value is only one:', m_endk_sample)
                m_endk.append(m_endk_sample[0])
            elif len(m_endk_sample) > 1:
                endk_top = [i['top'] for i in m_endk_sample]
                endk_x0 = [i['x0'] for i in m_endk_sample]
                x0_min = np.min(endk_x0)
                endk_top_max = np.max(endk_top)
                endk_top_min = np.min(endk_top)
                print('x0_min and top_max and top_min values are:', x0_min, endk_top_max, endk_top_min)
                for i in m_endk_sample:
                    if i['top']>= x_header['bottom']:
                        m_endk_sample2.append(i)
                print('m_endk_sample2 value is/are:', m_endk_sample2)
                if len(m_endk_sample2) == 1:
                    m_endk.append(m_endk_sample2[0])
                elif len(m_endk_sample2) > 1:
                    for j in m_endk_sample2:
                        if j['top'] == endk_top_max:
                            m_endk_sample3.append(j)
                    print('m_endk_sample3 values with max top:', m_endk_sample3)
                if len(m_endk_sample3) == 1:
                    m_endk.append(m_endk_sample3[0])
                if len(m_endk_sample3) > 1:
                    m_endk_sample3 = [i for i in m_endk_sample3 if i['x0'] == x0_min]
                    m_endk.append(m_endk_sample3[0])
                print('m_endk_sample3 value is:', m_endk_sample3[0])
            print('m_endk value is', m_endk)
            return m_endk

        def multi_column_values_kv(m_endc_values, x_header, x_endk):
            m_endc = []
            m_endc_sample = []
            m_endc_sample2 = []
            print('m_endc_values, x_header, x_endk:', m_endc_values, x_header, x_endk)
            if len(x_header) == 1:
                x_header = x_header[0]
            else:
                x_header = x_header

            if len(x_endk) == 1:
                x_endk = x_endk[0]
            else:
                x_endk = x_endk
            for i in m_endc_values:
                if x_header['x0']<i['x0'] and i['bottom'] <= x_endk['top'] and x_header['top']<=i['top']:
                    print('below header above key values are:', i)
                    m_endc_sample.append(i)
                elif  x_header['x0']<i['x0'] and i['bottom'] <= x_endk['top'] or x_header['top']<=i['top']:
                    print('above key values are:', i)
                    m_endc_sample.append(i)
            print('no of matching values:', m_endc_sample)
            if len(m_endc_sample) == 0:
                z_value = []
                return z_value
            elif len(m_endc_sample) == 1:
                print('endc_sample value is only one:', m_endc_sample)
                m_endc.append(m_endc_sample[0])
            elif len(m_endc_sample) > 1:
                endc_top = [i['top'] for i in m_endc_sample if i['x0'] > x_endk['x0']]
                endc_x0 = [i['x0'] for i in m_endc_sample if i['x0'] > x_endk['x0']]
                print('endc_top value is:', endc_top)
                print('endc_x0 value is:', endc_x0)
                print('x_endk x0 value is:', x_endk['x0'])
                x0_min = np.min(endc_x0)
                endc_top_min = np.min(endc_top)
                print('x0_min and top_min values are:', x0_min, endc_top_min)
                for i in m_endc_sample:
                    if i['top']>= x_header['top'] or i['x0'] == x0_min:
                        m_endc_sample2.append(i)
                print('m_endc_sample2 minx0 and great than header top:', m_endc_sample2)
                if len(m_endc_sample2) == 0:
                    for j in m_endc_sample:
                        if j['x0'] == x0_min:
                            m_endc_sample2.append(j)
                    print('m_endc_sample2 values with min x0:', m_endc_sample2)
                elif len(m_endc_sample2) == 1:
                    m_endc.append(m_endc_sample2[0])
                if len(m_endc_sample2) > 1:
                    sample2 = [i for i in m_endc_sample2 if i['x0'] == x0_min and i['top'] == endc_top_min]
                    if len(sample2) == 1:
                        m_endc_sample2 = sample2
                    elif len(sample2) == 0:
                        sample2 = [i for i in m_endc_sample2 if i['x0'] == x0_min]
                        sample2 = [i for i in sample2 if i['top'] >= endc_top_min]
                        m_endc_sample2 = sample2
                    m_endc.append(m_endc_sample2[0])
                    print('sample2 value is:', m_endc_sample2[0])        
            print('m_endc value is', m_endc)
            return m_endc

        def list2table(res_obj):
            temp_record_end_index = []
            for sub_res_obj in range(len(res_obj)):
                temp_obj = [i for i in res_obj[sub_res_obj] if i!='']
                if len(temp_obj) == 0:
                    temp_record_end_index.append(sub_res_obj)    
            return prepare_df(temp_record_end_index,res_obj)

        # def prepare_df(index_obj,dataobj):
        #     finalrecord = []    
        #     for i in range(len(index_obj)+1):
        #         if i==0:    
        #             rec_t = dataobj[:index_obj[i]]
        #         elif i == len(index_obj):
        #             rec_t = dataobj[index_obj[i-1]+1:]
        #         else:
        #             rec_t = dataobj[index_obj[i-1]+1:index_obj[i]]
        #         rec_t_df = pd.DataFrame(rec_t)
        #         temprecord = []
        #         for col_index in range(rec_t_df.shape[1]):
        #             temprecord.append(" ".join(rec_t_df[col_index].to_list()))
        #         finalrecord.append(temprecord)
        #     return pd.DataFrame(finalrecord)

        def prepare_df(index_obj,dataobj):
            finalrecord = []    
            if len(index_obj) != 0:
                for i in range(len(index_obj)+1):
            #         print('index_obj range value is:', len(index_obj)+1)
                    if i==0:    
                        rec_t = dataobj[:index_obj[i]]
                        print('i_0 rec_t value is:', rec_t)
                    elif i == len(index_obj):
                        rec_t = dataobj[index_obj[i-1]+1:]
                        if len(rec_t) == 0:
                            rec_t = ''
                        print('i_io re_t value is:', rec_t)
                    else:
                        rec_t = dataobj[index_obj[i-1]+1:index_obj[i]]
                        print('else rec_t value is:', rec_t)
                    if rec_t == '':
                        continue
                    else:
                        rec_t_df = pd.DataFrame(rec_t)

                    print('rec_t_df value is:', rec_t_df)
                    temprecord = []
                    for col_index in range(rec_t_df.shape[1]):
                        temprecord.append(" ".join(rec_t_df[col_index].to_list()))
                    print('temprecord value is:', temprecord)
                    finalrecord.append(temprecord)
            else:
                return pd.DataFrame(dataobj)
            print('finalrecord value is:', finalrecord)
            return pd.DataFrame(finalrecord)

        def multi_line_rows(word, words_list):
            sample_words = words_list
            multi_row_value = word
            multi_row_split = multi_row_value.split('\n')
            count_n = len(multi_row_split)-1
            count_wrds = len(multi_row_split)
            concat_row = []
            individual_row = multi_row_split
            individual_value = []
            temp_value = []
            cn = 0
            for q in range(count_wrds):
                temp_search = list(filter(lambda x:cleanse_key(individual_row[q]) in cleanse_key(x['text']), sample_words))
                temp_match = [i for i in temp_search if SM(None, cleanse_key(i['text']), cleanse_key(individual_row[q])).ratio() > 0.95]
                print('temp_match value is:', temp_match)
                if len(temp_match) == 0 and q<count_n:
                    new_key = individual_row[q]+ ' ' + individual_row[q+1]
                    temp_match = [i for i in temp_search if SM(None, cleanse_key(i['text']), cleanse_key(new_key)).ratio() > 0.95]
                print('temp_match value is:', temp_match)
                if len(temp_match) > 0:
                    individual_value.insert(q,temp_match)
            print('individual_value is:', individual_value)
            if len(individual_value) == 0:
                return
            id_value = int(len(individual_value)/2)+1
            if len(individual_value) % 2 == 0:
                ev_id = [i for i in range(0,id_value,2)]
                print(ev_id)
                for i in ev_id:
                    if i < id_value-1:
                        temp_value.insert(i, multi_single(individual_value[i], individual_value[i+1]))
                        individual_value.pop(i+1)
                    print('temp_value is:', temp_value)
                    if i == id_value-1:
                        temp_value.insert(i-1, multi_single(individual_value[i-1], individual_value[i]))
                print('temp_value is:', temp_value)
                if len(temp_value) == 2:
                    temp_value = multi_single(temp_value[0], temp_value[1])
                print('final temp_value is:', temp_value)
            elif len(individual_value) % 2 != 0:
                od_id = [i for i in range(0, id_value,1)]
                print(od_id)
                for i in od_id:
                    if i < id_value-1:
                        temp_value.insert(i, multi_single(individual_value[i], individual_value[i+1]))
                        individual_value.pop(i+1)
                    print("temp_value is:", temp_value)
                    if i == id_value-1:
                        temp_value.insert(i, individual_value[i])
                    print('temp value is:', temp_value)
                    if len(temp_value) == 2:
                        temp_value = multi_single(temp_value[0], temp_value[1])
            print('final temp_value is:', temp_value[0])
            return temp_value[0]


        def get_respective_fields(header, ending_key, ending_column, text, pg_index):
            headers_kv = header_kv(header, text, pg_index)
            print(' headers_kv value is:', headers_kv)
            if headers_kv == None:
                return

            endk_kv = ending_key_kv(ending_key, text, headers_kv, pg_index)
            if endk_kv == None:
                return

            endc_kv = ending_column_kv(ending_column, text, headers_kv, endk_kv[0], pg_index)
            if endc_kv == None:
                return

            bbox_kv = [endc_kv['x0']-14.9, endk_kv[0]['top']-1, endc_kv['x1']+7, endk_kv[0]['bottom']+4]
        #     print('bbox_kv value is:', bbox_kv)
        #     bbox_tb = [endk_kv[0]['x0'], headers_kv['top'], endc_kv['x1'], endk_kv[0]['bottom']]
        #     print('bbox_tb value is:', bbox_tb)
            return headers_kv, endk_kv, endc_kv, bbox_kv
        #     return headers_kv, endk_kv, endc_kv

        def handle_multipleline(wordslist):
            words_temp = []
            for i in range(len(wordslist)):
                if len(words_temp) == 0:    
                    words_temp.append(wordslist[i])
                else:
                    diff_val = abs(words_temp[len(words_temp)-1]['x1'] - wordslist[i]['x1']) 
                    if diff_val >=0 and diff_val < 5:
                        words_temp[len(words_temp)-1]['text'] = words_temp[len(words_temp)-1]['text']+" "+wordslist[i]['text']
                    else:
                        words_temp.append(wordslist[i])
            return words_temp

        def hamdle_multiplelinerow(wordslist):
            words_temp = []
            for i in range(len(wordslist)):
                if len(words_temp) == 0:    
                    words_temp.append(wordslist[i])
                else:
                    diff_val = abs(words_temp[len(words_temp)-1]['x1'] - wordslist[i]['x0']) 

                    if diff_val >=0 and diff_val < 0.5:

                        words_temp[len(words_temp)-1]['text'] = words_temp[len(words_temp)-1]['text']+wordslist[i]['text']
                    else:
                        words_temp.append(wordslist[i])
            return words_temp

        def handle_multiple_row2(wordslist):
            words_temp = []                        
            for i in range(len(wordslist)):
                if len(words_temp) == 0:    
                    words_temp.append(wordslist[i])
                else:
                    diff_val = abs(words_temp[len(words_temp)-1]['x0'] - wordslist[i]['x0']) 
                    diff_val_doctop = abs(words_temp[len(words_temp)-1]['doctop'] - wordslist[i]['doctop']) 
                    if diff_val ==0 and diff_val_doctop<=11:
                        words_temp[len(words_temp)-1]['text'] = words_temp[len(words_temp)-1]['text']+' '+wordslist[i]['text']
                    else:
                        words_temp.append(wordslist[i]) 
            return words_temp


        def check_split(table_grid, crop_words):
            box = None
            for word in crop_words:
                #word = word['text'].strip()
                #print(word)

                found = False
                for row in table_grid[0]:

                    for grid_text in row:
                        if (grid_text == '' or grid_text == ' '):
                            pass
                        if grid_text == word['text'].strip():
                            current_box = word

                            found = True
                if found == False:
                    if box is None:
                        box =  word
                    else:
                        return(box['text']+' '+ word['text'])

            return 'No Split'


        def check_match(te,check):
            for li in te[0]:
                for word in li:
                    if word == check:
                        return True
            return False
        def df_new(dfb):
            print('DataFrame-1', dfb)
            l = dfb[0].str.split('  ')
            l = l.to_list()
            lp = []
            for i in l:
            #print(i)
            #print(type(i))

                if len(i) == 1:
                    ii = i[0].split(' ')
                    #print(ii)
                    if len(ii) == 1:
                        ii.append(' ')
                        lp.append(ii)
                    elif len(ii) > 2:

                        L1 = ii[0]
                        L2 = ii[1:]
                        sr = ' '.join(L2)

                        lp.append([L1, sr])

                    else:    
                        lp.append(ii)   
                else:
                    if len(i) > 2:


                        L1 = i[0]

                        L2 = i[1:]
                        sr = ' '.join(L2)


                        lp.append([L1, sr])



                    else:
                        lp.append(i)

            dfnew = pd.DataFrame(lp)
            dfnew = pd.concat([dfnew,dfb.loc[:,1:]], axis=1)

            return dfnew


        def lists_df(te):
            print('te value is:', te)
            df = pd.DataFrame(te[0])
            dfa = df.iloc[0:2,:]
            print('dfa value is:', dfa)
            dfb = df.iloc[2:-1,:]
            print('dfb value is:', dfb)
            dfc = df.iloc[-1::]
            print('dfc value is:', dfc)

            dfa.reset_index(inplace=True)
            dfa.drop('index',inplace=True,axis=1)
            print('dfa value is:\n',dfa)
            dfb.reset_index(inplace=True)
            dfb.drop('index',inplace=True,axis=1)
            print('dfb value is:\n', dfb)
            dfc.reset_index(inplace=True)
            dfc.drop('index',inplace=True,axis=1)
            print('dfc value is:\n', dfc)
            dfnew = df_new(dfb)
            print('dfnew value is:\n', dfnew)

            dffinal = pd.concat([dfa,dfnew,dfc], axis=0)
            dffinal.columns = range(dffinal.shape[1])
            print('dffinal value is:\n', dffinal)

            return dffinal
        def extract_tb(header, ending_key,ending_column):
            table_settings={
            "vertical_strategy": "text", 
            "horizontal_strategy": "text",
            "keep_blank_chars": True,}
            # box= []
            page = []
            buffer_table = []
            buffer_counter = 0
            # page_cnt = list(range(len(pdf.pages)))
            for pgno in page_cnt:

                text = pdf_ob.pages[pgno].extract_words(use_text_flow=True,keep_blank_chars=True)
                # print('1-text from PDF Phani', text)
                #++ Phani 
                print('==========================')
        #         print('Cluster Output', get_cluster_output(text))
        #         print('Hello')
                print('==========================')
                #-- Phani
                # p0 = pdf.pages[i]
                global pno
                pno = pgno + 1
                he = str(header).replace('\n','')
                endk = str(ending_key)
                endc = str(ending_column)
                out = get_respective_fields_tb(he, endk,endc,text)

                if out == None:
                    text2 = pdf_ob.pages[pgno].extract_words(extra_attrs=['y0', 'y1'],use_text_flow=True,keep_blank_chars=True)
                    process_index = datapreprocessing(text2)
                    process_text = extract_index_to_text(process_index,text2)
        #             global header, ending_key, ending_column
                    header_tb = he.replace(" ",'') if type(he) == str else he
                    ending_key_tb = endk.replace("\n","").replace(" ",'') if type(endk) == str else endk
                    ending_column_tb = endc.replace(" ",'') if type(endc) == str else endc
                    out = get_respective_fields_tb(header_tb, ending_key_tb,ending_column_tb,process_text,buffer_counter)

                if out ==None:
                    pass

                elif out is not None:
                    #phani changes ++
        #             bbp = (31.4, 347.327, 725.016, 437.30600000000004)
                    print('Phani bb - 1',out)
                    crop_page = pdf_ob.pages[pgno].within_bbox(out[-2], relative=False)
                    crop_words = crop_page.extract_words(x_tolerance=3, y_tolerance=3,keep_blank_chars=True)
                    print('Phani bbox1', out[-2])
                    print('Phani crop words are: 1', crop_words)
                    # (31.4, 347.327, 725.016, 437.30600000000004)
        #             img = crop_page.to_image(resolution = 150)
        #             print('test1')
        #             tab = img.extract_tables(table_settings)
        #             print('Phani tab1', tab)

                    #phani changes --

                    # st.write(i+1)
                    #page.append(i)
                    # box.append(out[-1])

                    if out[-1] == 1:
                        box2 = out[-2]
                        # Phani ++

                        image =pdf_ob.pages[pgno].crop(box2)
        #                 image =pdf_ob.pages[pgno].crop(bbp)

                        # Phani --
                        te = image.extract_tables(table_settings)
                        if buffer_counter == 0 and len(te) != 0:
                            #hetitle = [[""] * len(te[0][0])]
                            #hetitle[0][0] = hetitle[0][0]+str(" --> page")+str(pgno+1)
                            #te=[hetitle+te[0]]#+str(" --> page")+str(pgno+1)
                            buffer_counter = buffer_counter+1                    
                        else:
                            buffer_counter = buffer_counter+1
                        buffer_table.append(te)
                    elif out[-1] == 0:                    
                        box2 = out[-2]
                        # Phani ++

                        image =pdf_ob.pages[pgno].crop(box2)
        #                 image =pdf_ob.pages[pgno].crop(bbp)

                        # Phani --                
                        te = image.extract_tables(table_settings)
                        if buffer_counter >0:
                            te_mr = tablemerge(te)
                            te_mr_v = te_mr.values.tolist()
                            te = [te_mr_v]
                            buffer_table.append(te)
                            bt_result = []
                            for tablesplit in buffer_table:
                                bt_result.append(list2table(tablesplit[0]))
                            bt_result.append(pd.DataFrame())    
                            page.append(bt_result)                    
                        else:
                            if len(te) > 0:
                                print('\n')
                                print('te value is:', te)
                                print('\n')
                                # N ++
                                check = check_split(te, crop_words)
                                print('check-1', check)
                                matchfound = False
                                if check != 'No Split':
                                    matchfound = check_match(te, check)

                                if matchfound == True:
                                    print('Match Found')
                                    te = lists_df(te)
                                # N --
                                te[0][0][0]=te[0][0][0]+str(" --> page")+str(pgno+1)
                                print('\n')
                                print('te new value is:', te)
                                print('\n')
                                # N ++
                                if matchfound == True:
                                    print("Why I am here")
                                    page.append(te)
                                else:
                                    print('I am here')
                                    temp_result_df = list2table(te[0])
                                    check_values_merge = temp_result_df.iloc[:,-1].to_list()[1:]
                                    if len([i for i in check_values_merge if len(i.split(" "))>2])>0:
                                        print("entered................")
                                        page.append(pd.DataFrame(te[0]))
                                    else:
                                        page.append(list2table(te[0]))
                                #N --

                        #print(te)


                    # st.write(box2)


            return page

        def multi():

            s=[]
            global he
            global endk
            global endc

            for i in range(len(rowss)):
                he, endk,endc = rowss[i]
                he = str(he).lower()
                endk = str(endk).lower()
                endc = str(endc).lower()

                try:
                    test_obj = extract_tb(he,endk,endc)
                except Exception as err:
                    test_obj = [pd.DataFrame({'PDF Table Header':"Unable to extract: {}".format(err)},index=[0])]
                # print('test_obj value is', test_obj)
                s.append(test_obj)
            # print('s value is', s)
            return s

        def extract_tru(header,ending_key,ending_column,lengths):

            table_settings={
            "vertical_strategy": "text", 
            "horizontal_strategy": "text",
            "keep_blank_chars": True,}
            # box= []
            page = []
            pdf = pdf_ob
            pdf_ob2 = pdf_ob
            page_cnt2 = list(range(len(pdf_ob2.pages)))
            for i in page_cnt2:

                text = pdf.pages[i].extract_words(use_text_flow=True,keep_blank_chars=True)
                # p0 = pdf.pages[i]

                out = get_respective_fields_tb(header,ending_key,ending_column,text)

        # ## Phani/Tapas +++

        #         list_of_grids = extract_table_grids(out[-2], text) # All the bounding boxes in table boudary should come here 
        #         ### elements in grid should be (text, box) # box means  centroid
        #         ### now call get_cluster_output 
        # ## Centroid calculation +++       
        #     def get_centre_coordinates_data(boxes, shape):
        #     '''
        #     This Function is used to generate the centre coordinates of all the character detected by the pytesseract. We have stored the character,
        #     character bounding box coordinates and the centre coordinates.
        # :param boxes: Pytesseract output or the character information provided by pytesseract.
        # :param shape: Original Image Array shape.
        # :return: List_of_bbox contains character,character bounding box coordinates and the centre coordinates for each character detected.
        #     '''
        # list_of_bbox = []
        #     h, w, _ = shape
        #     if len(boxes) > 2:
        #         for b in boxes.splitlines():
        #             b = b.split(' ')
        #             charac = ord(b[0])
        #             if charac > 64 and charac < 123 or charac > 47 and charac < 58:
        #                 # Finding the centre x and centre y coordinate for all the character so that we can perform clustering on that.
        # cent_x, cent_y = (int(b[1]) + int(b[3])) // 2, ((h - int(b[2])) + (h - int(b[4]))) // 2
        # list_of_bbox.append([b[0], [int(b[1]), h - int(b[4]), int(b[3]), h - int(b[2])], [cent_x, cent_y]])
        #     return list_of_bbox
        # ## Centroid calculation --


        # ## Phani/Tapas ---

                ## 
                if out == None:
                    pass

                else:
                    #phani changes ++
                    print('Phani bb - 2',out)
                    crop_page = pdf_ob.pages[i].within_bbox(out[-2], relative=False)
                    crop_words = crop_page.extract_words(x_tolerance=3, y_tolerance=3,keep_blank_chars=True)
                    print('Phani crop words are: 2', crop_words)

                    img = crop_page.to_image(resolution = 150)
                    print('test2')
                    tab = img.extract_tables(table_settings)
                    print('Phani tab2', tab)
                    #phani changes --
                    # st.write(i+1)

        #             page.append(i)
                    # box.append(out[-1])
                    box2 = out[-1]
                    truncate = list(box2)
                    truncate[2] = truncate[2]+lengths
                    box3 = tuple(truncate)

                    image =pdf.pages[i].crop(box3)
                    te = image.extract_tables(table_settings)
                    # st.write(te)
                    te[0][0][0]=te[0][0][0]+str(" --> page")+str(i+1)
                    # st.write(te)
                    ## Phani +++
                    print('Phani te', te)
                    ## Phani ---
                    page.append(te)




            return page

        def tab_ext(ss = None):
            import random
            rn_tb = random.randrange(0, 10000,3)
            global rowss
            # print('ss value is', ss)
            if ss is not None:
                print("Table Extracton Started Please Wait...")
                rowss = ss.values.tolist()
                table_list = multi()
                # print('table list value is:', table_list)
                df_tb = []

                sample_te = [1, 3, 4]
                ref_dataframe = pd.DataFrame(sample_te)
                for i in range(len(table_list)):
                    tables = table_list[i]
                #     print(type(tables))
                    for j in range(len(tables)):
                        tb_df = tables[j]
                #         print(type(tb_df))
                        if type(tb_df) == type(ref_dataframe):
                            df_tb.append(tb_df)
                        else:
                            for k in range(len(tb_df)):
                                tb_df1 = tb_df[k]
                    #             print(type(tb_df1))
                                if type(tb_df1) == type(ref_dataframe):
                                    print(type(tb_df1))
                                    df_tb.append(tb_df1)
                if len(df_tb) > 0:
                    result1 = pd.concat(df_tb, ignore_index=True)
                    result1.dropna(how = 'all', inplace = True)
                    result1.fillna('NaN', inplace = True)
                    try:
                        for i in result1:
                            if int(i)>=1:
                                result1[i] = result1[i].str.strip()
                                result1[i] = result1[i].str.replace('  ',' | ').replace("   "," | ")
                                table_xl = result1.to_excel('FullTable'+f'{rn_tb}'+'.xlsx', index = False, header = False)
                        print("Result1 value is:", result1)
                    except Exception as err:
                        result1 = pd.DataFrame({'Table':"Unable to extract: because of {} error".format(err)},index=[0])

                else:
                    result1 = pd.DataFrame(df_tb, index= None)
                    print('Result1 value is:', result1)
                    table_xl = result1.to_excel('FullTable'+f'{rn_tb}'+'.xlsx', index = False, header = False)
            elif ss is None:
                print("No keys to extract table")
                result1 = pd.DataFrame({"Message":"No keys to extract table"},index=[0])
            return result1



        def remove_repeted_index(set_index):
            final_set_index_draft = []
            temp_list = []
            for _index in range(len(set_index)):
                if _index == 0:
                    temp_list = set_index[_index]
                else:    
                    res = [i for i in set_index[_index] if i not in temp_list]
                    if (res == []) & (_index == len(set_index)-1):
                        final_set_index_draft.append(temp_list)
                    elif (res != []) & (_index == len(set_index)-1):
                        final_set_index_draft.append(temp_list)
                        final_set_index_draft.append(set_index[_index])
                    elif res == []:
                        continue
                    else:
                        final_set_index_draft.append(temp_list)
                        temp_list = set_index[_index]
            return final_set_index_draft


        def datapreprocessing(pagedata):    
            #modified_pagedata = []
            final_set_index = []
            for _index in range(len(pagedata)):
                pagedata[_index]['x0']
                y1_Value = pagedata[_index]['y1']
                x1_Value = pagedata[_index]['x1']
                set_values = [_index]
                for _secondindex in range(_index+1,len(pagedata)):
                    #print("{}:::,{}::,{}::,{}".format(_secondindex,x1_Value,(pagedata[_secondindex]['x0'] - x1_Value)<7,(pagedata[_secondindex]['y0'] - y0_Value)<5))
                    if (abs(pagedata[_secondindex]['x0'] - x1_Value)<7) & (abs(pagedata[_secondindex]['y1'] - y1_Value)<5) :
                        #print(_)
                        x1_Value = pagedata[_secondindex]['x1']
                        y1_Value = pagedata[_secondindex]['y1']
                        set_values.append(_secondindex)
                    else:
                        pass

                final_set_index.append(set_values)

            final_set_index_draft = remove_repeted_index(final_set_index)
            final_set_index_draft_v2 = []    
            for _index in range(len(final_set_index_draft)):
                rec_index = final_set_index_draft[_index]
                temp_list = final_set_index_draft[_index]
                for _secondindex in range(_index+1,len(final_set_index_draft)):
                    sec_rec_index = final_set_index_draft[_secondindex]        
                    if abs(pagedata[rec_index[-1]]['x1'] - pagedata[sec_rec_index[-1]]['x1'])< 0.5:
                        if abs(pagedata[rec_index[-1]]['y0'] - pagedata[sec_rec_index[-1]]['y0'])< 14:
                            temp_list=temp_list+sec_rec_index
                        else:
                            pass
                    elif abs(pagedata[rec_index[-1]]['x1'] - pagedata[sec_rec_index[0]]['x1'])< 0.5:
                        if abs(pagedata[rec_index[-1]]['y0'] - pagedata[sec_rec_index[-1]]['y0'])< 14:
                            temp_list=temp_list+sec_rec_index
                        else:
                            pass
                    else:
                        pass
                final_set_index_draft_v2.append(temp_list)


            final_set_index_draft_v2 = remove_repeted_index(final_set_index_draft_v2)
            final_set_index_draft_v3 = []    
            for _index in range(len(final_set_index_draft_v2)):
                rec_index = final_set_index_draft_v2[_index]
                temp_list = final_set_index_draft_v2[_index]
                for _secondindex in range(_index+1,len(final_set_index_draft_v2)):
                    sec_rec_index = final_set_index_draft_v2[_secondindex]        
                    if abs(pagedata[rec_index[0]]['x0'] - pagedata[sec_rec_index[0]]['x0'])< 0.5:
                        if abs(pagedata[rec_index[0]]['y0'] - pagedata[sec_rec_index[0]]['y0'])< 14:
                            temp_list=temp_list+sec_rec_index
                        else:
                            pass
                    else:
                        pass
                final_set_index_draft_v3.append(temp_list)
            final_set_index_draft_v3 = remove_repeted_index(final_set_index_draft_v3)
            final_set_index_draft_v4 = []

            lookup_list = []
            for _index in range(len(final_set_index_draft_v3)):
                rec_index = final_set_index_draft_v3[_index]
                temp_list = final_set_index_draft_v3[_index]        
                for _secondindex in range(_index+1,len(final_set_index_draft_v3)):
                    sec_rec_index = final_set_index_draft_v3[_secondindex]
                    if len(set(temp_list).intersection(set(sec_rec_index)))>0:
                        temp_list = temp_list+list(set(sec_rec_index)-set(temp_list))
                        lookup_list.append(sec_rec_index)
                res = rec_index not in lookup_list
                if res:
                    final_set_index_draft_v4.append(temp_list)

            return final_set_index_draft_v4

        def extract_index_to_text(process_index,pagedata): 
            process_wordlist = []
            for _index_rec in process_index:
                text_info = ''
                rec_temp = ''
                for _index_val in _index_rec:
                    if rec_temp == '':
                        rec_temp = pagedata[_index_val]
                    else:
                        rec_cmp = pagedata[_index_val]
                        rec_temp['x0'] = min(rec_temp['x0'],rec_cmp['x0'])
                        rec_temp['x1'] = max(rec_temp['x1'],rec_cmp['x1'])
                        rec_temp['y0'] = min(rec_temp['y0'],rec_cmp['y0'])
                        rec_temp['y1'] = max(rec_temp['y1'],rec_cmp['y1'])
                        rec_temp['top'] = min(rec_temp['top'],rec_cmp['top'])
                        rec_temp['bottom'] = max(rec_temp['bottom'],rec_cmp['bottom'])
                        rec_temp['doctop'] = min(rec_temp['doctop'],rec_cmp['doctop'])
                    text_info = text_info+pagedata[_index_val]['text']
                text_info = text_info.replace(" ","")
                rec_temp['text'] = text_info
                #process_wordlist.append({"text":text_info})
                process_wordlist.append(rec_temp)
            return process_wordlist

        def get_respective_fields_tb(header,ending_key,ending_column,words_list,buffercounter=0):
                #print('Actual header:', header)
                flag_multiple_page = buffercounter
                if '\n' in header:
                    sample_header = header
                    indxh = int(header.index('\n'))
                    #print('index position of /n is --->', indxh)
                    header = header[:indxh]
                    header = header.rstrip()
                    #print('Local-----New header value is --->', header)
                    header1 = sample_header[indxh+1:]
                    #print('Local-----header sliced value is --->', header1)
                    headers1 = list(filter(lambda x:header in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    #print(f'Local-----headers matched after searching page: {pno}', headers1)
                    headers =  [i for i in headers1 if header == i['text'].replace(' (', '(').replace('  ', ' ').lower().rstrip()]
                    #print(f'Local-----headers value selected in page: {pno}', headers)
                    if len(headers) == 0 and '\n' in header1:
                        indxh1 = int(header1.index('\n'))
                        header1 = header1[:indxh1]
                        header1 = header1.rstrip()
                        header = header+' '+header1
                        headers1 = list(filter(lambda x:header in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                        #print(f'Local1-----headers matched after searching page: {pno}', headers1)
                        headers =  [i for i in headers1 if header == i['text'].replace(' (', '(').replace('  ', ' ').lower().rstrip()]
                        #print(f'Local1-----headers value selected in page: {pno}', headers)
                else:
                    headers1 = list(filter(lambda x:header in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    #print(f'headers matched after searching page: {pno}', headers1)
                    headers =  [i for i in headers1 if header == i['text'].replace(' (', '(').replace('  ', ' ').lower().rstrip()]
                    #print(f'Selected header value in page: {pno}', headers)



                if (len(headers) == 0) & (flag_multiple_page >0):
                    #print('headers_length and flag_multiple_page values are:', len(headers), len(flag_multiple_page))
                    headers = list(filter(lambda x:header.find(x['text'].lower())>=0,words_list))
                    #print('value of headers of ng code:', headers)
                    if headers == []:
                        new_header_key = header+'continued'
                        headers = list(filter(lambda x:x['text'].lower().find(new_header_key)>=0,words_list))
                        #print('headers value if headers=[]', headers)
                        #flag_multiple_page = 1
                if len(headers) == 0:
                    return 

                # print('Actual ending_key:', ending_key)
                if '\n' in ending_key:
                    sample_key = ending_key
                    indxk = int(ending_key.index('\n'))
                    #print('Local-----index position of /n is --->', indxk)
                    ending_key = ending_key[:indxk]
                    ending_key = ending_key.rstrip()
                    #print('Local-----New ending_key value is --->', ending_key)
                    ending_key1 = sample_key[indxk+1:]
                    #print('Local-----ending_key sliced value is --->', ending_key1)
                    ending_keys = list(filter(lambda x:ending_key.replace('*','').rstrip('∆') in x['text'].replace('  ', ' ').replace(' (', '(').rstrip('∆').lower(),words_list))
                    #print(f'Local-----ending_keys matched after searching page: {pno}', ending_keys)
                    ending_keys =  [i for i in ending_keys if ending_key.replace('*','').rstrip('∆') == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip('∆').rstrip()]
            #         print(f'Local-----ending_keys value selected in page: {pno}', ending_keys)
                    if len(ending_keys) == 0 and '\n' in ending_key1:
                        indxk1 = int(ending_key1.index('\n'))
                        ending_key1 = ending_key1[:indxk1]
                        ending_key1 = ending_key1.rstrip()
                        ending_key = ending_key+' '+ending_key1
                        ending_keys = list(filter(lambda x:ending_key.replace('*','').rstrip('∆') in x['text'].replace('  ', ' ').replace(' (', '(').rstrip('∆').lower(),words_list))
                        #print(f'Local1-----ending_keys matched after searching page: {pno}', ending_keys)
                        ending_keys =  [i for i in ending_keys if ending_key.replace('*','').rstrip('∆') == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip('∆').rstrip()]
                        #print(f'Local1-----ending_keys value selected in page: {pno}', ending_keys)
                else:    
                    ending_keys = list(filter(lambda x:ending_key.replace('*','').rstrip('∆') in x['text'].replace('  ', ' ').replace(' (', '(').rstrip('∆').lower(),words_list))
                    #print(f'ending_keys matched after searching page: {pno}', ending_keys)
                    ending_keys =  [i for i in ending_keys if ending_key.replace('*','').rstrip('∆') == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip('∆').rstrip()]
                    #print(f'ending_keys value selected in page: {pno}', ending_keys)

                if len(ending_keys) == 0:
                    new_ending_keys_1 = list(filter(lambda x:ending_key.find(x['text'].lower())>=0,words_list))
                    #if ending_keys == []:
                    new_ending_key = 'continuedonnextpage'
                    new_ending_keys_2 = list(filter(lambda x:x['text'].lower().find(new_ending_key)>=0,words_list))
                        #if ending_keys == []:
                        #    ending_keys = list(filter(lambda x:x['text'].lower().find(ending_key)>=0,words_list))
                    if (new_ending_keys_1 == []) and (new_ending_keys_2 != []):
                        ending_keys = new_ending_keys_2
                        flag_multiple_page = 1
                    elif (new_ending_keys_1 == []) & (new_ending_keys_2 == []):
                        ending_keys = new_ending_keys_2
                        flag_multiple_page = 1
                    elif (new_ending_keys_1 != []) & (new_ending_keys_2 == []):
                        ending_keys = new_ending_keys_1
                        flag_multiple_page = 0
                    else:
                        ending_keys = new_ending_keys_2
                        flag_multiple_page = 1

                else:
                    flag_multiple_page = 0

                if len(ending_keys)>1:
                    ending_keys = [i for i in ending_keys if i['doctop'] > headers[0]['doctop']]
                    ending_k_min = [i['bottom'] for i in ending_keys]
                    endkeymin = np.min(ending_k_min)
                    if len(ending_keys) > 1:
                        ending_keys = [i for i in ending_keys if i['bottom']==endkeymin]
                    else:
                        pass

                if len(ending_keys) == 0:
                    return

                # print('Actual ending_column:', ending_column)
                if '\n' in ending_column:
                    sample_column = ending_column
                    indxc = int(ending_column.index('\n'))
                    #print('index position of /n is --->', indxc)
                    ending_column = ending_column[:indxc]
                    ending_column = ending_column.rstrip()
                    #print('New ending_column value is --->', ending_column)
                    ending_column1 = sample_column[indxc+1:]
                    #print('Local-----ending_column sliced value is --->', ending_column1)
                    ending_columns = list(filter(lambda x:ending_column.rstrip('*') in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                    #print(f'Local-----ending_columns matched after searching page: {pno}', ending_columns)
                    ending_columns =  [i for i in ending_columns if ending_column.rstrip('*') == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip()]
                    #print(f'Local-----ending_columns value selected in page: {pno}', ending_columns)
                    if len(ending_columns) == 0 and '\n' in ending_column1:
                        indxc1 = int(ending_column1.index('\n'))
                        ending_column1 = ending_column1[:indxc1]
                        ending_column1 = ending_column1.rstrip()
                        ending_column = ending_column+' '+ending_column1
                        ending_columns = list(filter(lambda x:ending_column.rstrip('*') in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                        #print(f'Local1-----ending_columns matched after searching page: {pno}', ending_columns)
                        ending_columns =  [i for i in ending_columns if ending_column.rstrip('*') == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip()]
                        #print(f'Local1-----ending_columns value selected in page: {pno}', ending_columns)
                else:
                    ending_columns = list(filter(lambda x:ending_column in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                    #print(f'ending_columns matched after searching page: {pno}', ending_columns)
                    ending_columns =  [i for i in ending_columns if ending_column == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip() ]
                    #print(f'ending_columns selected in page: {pno}', ending_columns)
                    if len(ending_columns) > 1:
                        ending_columns = [i for i in ending_columns if ending_keys[0]['top'] > i['top']]

                if len(ending_columns) == 0:
                    ending_columns = list(filter(lambda x:ending_column.find(x['text'].lower())>=0,words_list))
                    new_ending_column_processed ="".join([text_value['text'].lower() for text_value in ending_columns])
                    if new_ending_column_processed == ending_column:
                        ending_columns = [ending_columns[0]]
                    #ending_columns =  [i for i in ending_columns if ending_column == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip() ]
                if len(ending_columns) == 0:
                    return 

                x_head=headers[0]

                ending_pts=[i for i in ending_columns if i['x0']>x_head['x0']]
                #print(f'ending_pts in page: {pno}', ending_pts)
                if len(ending_pts) == 0:
                    return 
                ending_x_min=[i['x0'] for i in ending_pts]
                #print(pno, ending_x_min)
                ending_x_min=sorted(list(set(ending_x_min)))[0]
                #print(pno, ending_x_min)
                min_x0=ending_x_min
                #print(min_x0)
                pt=None
                for point in ending_pts:
                    if point['x0']==min_x0:
                        if pt is None:
                            pt=point
                        elif pt['top']>point['top']:
                            pt=point
                header= headers[0]
                ending_keys=ending_keys[0]
            #     bbox=(header['x0'],header['top'],pt['x1'],ending_keys['bottom']+2)
                bbox=(header['x0'],header['top'],pt['x1'],ending_keys['bottom']+2)
                bbox_kv=(pt['x0']-12,ending_keys['top']-0.1,pt['x1']+6,ending_keys['bottom']+4)
            #     bbox=(pt['x0'],ending_keys['top'],pt['x1'],ending_keys['bottom']+2)
            #     words_list_filtered=[word for word in words_list if word['x0']>=bbox[0] and word['x1']<=bbox[2] and word['top']>=bbox[1] and word['bottom']<=bbox[3]]
                return header,ending_keys,pt,bbox_kv,bbox,flag_multiple_page


        #import time

        #header, ending_key,ending_column = he, endk,endc

        def run_extraction(file_name, reference_name):
        #     global ss
            global reference
            global table_headers
            global page_cnt
            import random
            import urllib.request
            global patyh
            global pdf_ob
            table_input = None
            # print(file_name)
            rn1 = random.randrange(0, 10000, 3)
            # response = urllib.request.urlopen(file_name)
            # file = open('filename'+ str(rn1) + ".pdf" , 'wb')
            # file.write(response.read())
            # file.close()
            # except urllib.error.URLError:
            #     print("\n------------------The file is not found !------------------------------")
            #     print("------------------Please check the url------------------------------")
            #     return "------------File not found pls check the url-------------"
            # pdf_ob= pdfplumber.open(f'filename{rn1}.pdf')
            pdf_ob = pdfplumber.open(file_name)
            page_cnt = list(range(len(pdf_ob.pages)))
            # reference = pd.DataFrame(reference_name)
            reference = pd.read_csv(reference_name)
            # reference["Classification category"].fillna("NaN", inplace = True)
            reference['extraction_type'] = reference['Extraction Type']
            print(reference)
            table_input = reference.loc[reference['extraction_type'] == 'whole-table']
    #             print('table_input is:', table_input)
            if len(table_input) > 0:
                table_input['Table_ending_key'] = table_input['Table_ending_key'].apply(
                    lambda x: x.replace(' (', '(').replace("d_", '') if type(x) == str else x)
                table_input = table_input[['PDF Table Header','Table_ending_key', 'Table_ending_column']]
                table_input = table_input.reset_index(drop= True)
                table_input['Table_ending_column'] = table_input['Table_ending_column'].apply(
                    lambda x: x.replace("\n",'') if type(x) == str else x)
                result_tb = tab_ext(table_input)
                if len(result_tb) > 0:
                        result_tb = result_tb
                else:
                    # print("No keys to extract Full_table")
                    result_tb = pd.DataFrame({"Message":"Please check the keys"},index=[0])
            else:
    #                 print("No keys to extract Full_table")
                result_tb = pd.DataFrame({"Message":"No keys to extract Full_table"},index=[0])

            specific_column_input = reference.loc[reference['extraction_type'] == 'whole-column']
    #             print('specific_column_input is:', specific_column_input)
            if len(specific_column_input) > 0:
                specific_column_input = specific_column_input[['PDF Table Header','PDF Key','columns_to_extract','Table_ending_key','extraction_type']]
                ex_table = specific_column_input[['PDF Table Header','Table_ending_key','columns_to_extract',]]
                ex_specific = specific_column_input[['PDF Table Header','PDF Key','columns_to_extract']]
                res = tab_ext(ex_table)
                if len(res) > 0:
                    ex_specific_values = ex_specific.values.tolist()
                    for i in range(len(ex_specific_values)):
                        PDF_header, PDF_Key, columns_extract = ex_specific_values[i]
                    df_cfc = [res]
                    if PDF_header == PDF_Key:
                        PDF_Key = res[0][0]
                    userreqcols = [PDF_Key, columns_extract]
                    filtered_table = []
                    for userreqcol in userreqcols:
                        if userreqcol != '':  
                            for rows in range(4):
                                for columns in res:
                                    # print('rows, columns, req', rows, columns, userreqcol)
                                    usrcolindex= res.loc[columns][res.loc[rows].isin([userreqcol])].index
                                    # print('usrcolindex value is:', usrcolindex)
                                    if len(usrcolindex) > 0:
                                        # print('appended value is:', res[[usrcolindex[0]]])
                                        filtered_table.append(res[[usrcolindex[0]]])
                                        break
    #                     print('filtered_table value is:\n',filtered_table)
                    if len(filtered_table) > 0:
                        result_ex = pd.concat(filtered_table, axis=1, ignore_index=False)
                        table_ex_xl = result_ex.to_excel('Specific_column'+f'{rn1}'+'.xlsx', index = False, header = False)
                    else:
                        result_ex = pd.DataFrame({"Message":"No keys to extract specific_column"},index=[0])
    #                         print('Result_ex value is:', result_ex)

                else:
    #                     print("No keys are matched to extract table")
                    result_ex = pd.DataFrame({"Message":"No keys to extract specific_column"},index=[0])
            else:
                output_path = 'file_not created'
                result_ex = pd.DataFrame({"Message":"No keys to extract specific_column"},index=[0])

            key_value_input = reference.loc[reference['extraction_type'].isin(['key-value', 'key-value-right', 'key-value-down', 'sub_table_extraction'])]   
            # print('key_value_input is:', key_value_input)
            if len(key_value_input) > 0:
                form_chk = reference.loc[reference['PDF Key'].isin(['Form','form', 'FORM'])] 
                if len(form_chk) > 0:
                    ################################
                    # part3 table extractor
                    pdf_path = file_name
                    # master_data = pd.DataFrame(master_data_dict)
                    # pdf_name=f'filename{rn1}.pdf'
                    global check_box_values
                    check_box_values = checkbox(file_name)
                    # check_box_values = checkbox(pdf_name)
    #                     print('check_box_values are:', check_box_values)
                    final_out, match_, head_ = part_3_table_subtable(file_name)
                    # final_out, match_, head_ = part_3_table_subtable(f'filename{rn1}.pdf')
                    # print('final_out columns:', final_out.columns)
                    for clmn in final_out.columns:
                        final_out[clmn] = final_out[clmn].astype(str).apply(lambda x: x.replace('|',''))
                    # print(final_out.to_dict())
                    if len(final_out) > 0:   
        #                 print(final_out)      
                        excel_out = final_out.to_excel('final_out_result.xlsx', index = False)
                    else:
    #                         print('No keys are matched')
                        final_out = pd.DataFrame({"Message":"No keys are matched"},index=[0])
                else:
                    key_value = reference.loc[reference['extraction_type'] == 'key-value']
                    key_value_specific = reference.loc[reference['extraction_type'].isin(['key-value-right', 'key-value-down'])] 
                    key_value_column_sub = reference.loc[reference['extraction_type'].isin(['sub_table_extraction'])]
                    global kv_reference
                    kv_reference = 0
                    if {'unique_ref_number'}.issubset(reference.columns):
                        key_value_account = key_value[key_value['unique_ref_number'].notnull()]
                        if len(key_value_account) > 0:
                            kv_reference = 1
                    else:
                        kv_reference = 0
                    if len(key_value) > 0 and kv_reference == 0:
                        key_value['dummy_unique'] = np.nan
                        key_value = key_value[['PDF Table Header','PDF Key', 'columns_to_extract','dummy_unique','extraction_type']]
                        key_value = key_value.reset_index(drop= True) 
                        key_value.drop_duplicates(inplace=True)
                        rowss_kv = key_value.values
                    elif len(key_value) > 0 and kv_reference == 1:
                        key_value = key_value_account[['PDF Table Header','PDF Key', 'columns_to_extract','unique_ref_number','extraction_type']]
                        key_value = key_value.reset_index(drop= True) 
                        key_value.drop_duplicates(inplace=True)
                        rowss_kv = key_value.values
                    else:
                        rowss_kv = [[0,0,0,0,0]]
    #                     print('\nkey_value is:\n', key_value)
                    if len(key_value_specific) > 0:
                        if {'PDF Table Header'}.issubset(key_value_specific):
                            key_value_specific = key_value_specific[['PDF Table Header','PDF Key','PDF Key','PDF Key','extraction_type']]
                        else:
                            key_value_specific['dummy_header'] = np.nan
                            key_value_specific = key_value_specific[['dummy_header','PDF Key','PDF Key','PDF Key','extraction_type']]
                        key_value_specific = key_value_specific.reset_index(drop= True) 
                        key_value_specific.drop_duplicates(inplace=True)
                        rowss_sp = key_value_specific.values
                    else:
                        rowss_sp = [[0,0,0,0,0]]
                    if len(key_value_column_sub) > 0:
                        key_value_column = key_value_column_sub[['PDF Table Header','PDF Key', 'columns_to_extract','Table_ending_column','extraction_type']]
                        key_value_column = key_value_column.reset_index(drop= True) 
                        key_value_column.drop_duplicates(inplace=True)
                        rowss_c = key_value_column.values
                    else:
                        rowss_c = [[0,0,0,0,0]]
    #                     print('\nkey_value_specific:\n', key_value_specific)
    #                     print('\nrowss_kv value is:\n', rowss_kv)
    #                     print('\nrowss_sp value is:\n', rowss_sp)
            #         rowss1 = rowss_kv.insert(0,rowss_sp)
            #         rowss1 = np.stack([rowss_kv, rowss_sp]).reshape(-1)
                    rowss1 = np.concatenate([rowss_kv, rowss_sp, rowss_c])
    #                     print('\nrowss1 value is:\n', rowss1)
                    kv = []
                    dataframe_kv = []
                    for i in range(len(rowss1)):
                        h, endk,endc,uniq,ex_type = rowss1[i]
                        kv.append(extract_kv(h, endk,endc,uniq,ex_type))
                    for i in kv:
                        for j in i:
                            dataframe_kv.append(map(lambda x: x.capitalize(), j))

                    final_out = pd.DataFrame(dataframe_kv,columns=['PDF Table Header', 'PDF Key', 'columns_to_extract','Value','Page'])
                    final_out.fillna('NaN', inplace = True)
                    # final_out.drop_duplicates(inplace=True)
                    print(final_out)
                    if len(final_out) > 0:   
                        print(final_out)      
                        csv = final_out.to_csv('Key_values'+f'{rn1}'+'.csv', index=False)
                    else:
    #                         print('No keys are matched')
                        final_out = pd.DataFrame({"Message":"No keys are matched"},index=[0])

            else:
                output_path = 'file_not created'
                final_out = pd.DataFrame({"Message":"No keys to extract key values"},index=[0])

            return final_out, result_ex, result_tb
        #     return result_tb
            # return final_out

        def to_excel(df):
            output = BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            df.to_excel(writer, header=None,index=False, sheet_name='Sheet1')
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            format1 = workbook.add_format({'num_format': '0.00'}) 
            worksheet.set_column('A:A', None, format1)  
            writer.save()
            processed_data = output.getvalue()
            return processed_data

        # @st.cache
        def convert_df(df):
            # IMPORTANT: Cache the conversion to prevent computation on every rerun
            #  return df.to_csv(header=None,index=False,errors='ignore').encode('utf-8')
            return df.to_csv(index=False,errors='ignore').encode('utf-8')
        # def convert_df2(df):
        #      # IMPORTANT: Cache the conversion to prevent computation on every rerun
        #      return df.to_csv(index=False,encoding='utf-8',errors='ignore')
        sys.path.append(os.getcwd())



        def get_respective_fields_kv(header,ending_key,ending_column,words_list):
            global sample_header
            global sample_key
            global sample_column
            sample_header = header
            sample_key = ending_key
            sample_column = ending_column
            print('Actual header:', header)
            if '\n' in header:
                indxh = int(header.index('\n'))
                print('index position of /n is --->', indxh)
                header = header[:indxh]
                header = header.rstrip()
                print('Local-----New header value is --->', header)
                header1 = sample_header[indxh+1:]
                print('Local-----header sliced value is --->', header1)
                headers1 = list(filter(lambda x:header in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                print(f'Local-----headers matched after searching page: {pno}', headers1)
                headers =  [i for i in headers1 if header == i['text'].replace(' (', '(').replace('  ', ' ').lower().rstrip()]
                print(f'Local-----headers value selected in page: {pno}', headers)
                header1 = list(filter(lambda x:header1 in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                if len(header1) == 0:
                    return
                if len(headers) == 0 and '\n' in header1:
                    indxh1 = int(header1.index('\n'))
                    header1 = header1[:indxh1]
                    header1 = header1.rstrip()
                    header = header+' '+header1
                    headers1 = list(filter(lambda x:header in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    print(f'Local1-----headers matched after searching page: {pno}', headers1)
                    headers =  [i for i in headers1 if header == i['text'].replace(' (', '(').replace('  ', ' ').lower().rstrip()]
                    print(f'Local1-----headers value selected in page: {pno}', headers)
            else:
                print('header value is:', header)
                headers1 = list(filter(lambda x:header in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                print(f'headers matched after searching page: {pno}', headers1)
                headers =  [i for i in headers1 if header == i['text'].lower().replace(' (', '(').replace('  ', ' ').replace('(continued)','').rstrip()]
                print(f'Selected header value in page: {pno}', headers)
                if len(headers) == 0:
                    headers1 = list(filter(lambda x:header.replace(' ', '') in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    print(f'headers matched after searching page: {pno}', headers1)
                    headers =  [i for i in headers1 if header.replace(' ', '') == i['text'].lower().replace(' (', '(').replace('  ', ' ').replace('(continued)','').rstrip()]  
                if len(headers) == 0:
                    header_st= sample_header.split(' ')
                    headers1 = list(filter(lambda x:header_st[0] in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    headers1 = [i for i in headers1 if header_st[0] == i['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip()]
                    print(f'because headers not matched search after split in page: {pno}', headers1)
                    headers1_end = list(filter(lambda x:header_st[-1] in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    headers1_end = [i for i in headers1_end if header_st[-1] == i['text'].lower().replace(' (', '(').replace('(continued)','').replace('  ', ' ').rstrip()]
                    print(f'headers1_end value in page:{pno}', headers1_end)
                    if len(headers1_end) == 0:
                        return
                    if len(headers1)>1 and len(headers1_end)==1:
                        headers1 = [i for i in headers1 if i['bottom']<=headers1_end[0]['bottom']<=i['bottom']+20]
        #             elif len(headers1)>1 and len(headers1_end)>1:

                    if len(headers1)==1:
                        headers1[0]['x0']=sorted(list(set([headers1[0]['x0'], headers1_end[0]['x0']])))[0]
                        headers1[0]['x1']=sorted(list(set([headers1[0]['x1'], headers1_end[0]['x1']])))[-1]
                        headers1[0]['top']=sorted(list(set([headers1[0]['top'], headers1_end[0]['top']])))[0]
                        headers1[0]['bottom']=sorted(list(set([headers1[0]['bottom'], headers1_end[0]['bottom']])))[-1]
                        headers1[0]['text'] = headers1[0]['text'] + ' ' + headers1_end[0]['text']
                        headers1[0]['text'] = headers1[0]['text'].replace('  ',' ').rstrip()
                        headers = headers1
                        print(f'headers value after split in page:{pno}', headers)

                    if len(headers1)>1:
                        headers_start = [i['x1'] for i in headers1]
                        headersmin = np.min(headers_start)
                        headers = [i for i in headers1 if i['x0']<=headersmin]
                        print(f'headers matched after split and min start: {pno}', headers)
                        if len(headers)>1:
                            sample = headers
                            header_top = [i['top'] for i in headers]
                            headermin = np.min(header_top)
                            headers = [i for i in sample if i['top']==headermin]
                            print(f'headers matched after min start and min top:{pno}',headers)
                    else:
                        headers = headers1
                        print(f'headers matched after split in page {pno}', headers)

            if headers:
                sm_h = SM(None,f'{sample_header}',f"{headers[0]['text'].lower().replace(' (continued)','').replace('(continued)','').rstrip()}").ratio()
                print('smh value is:', sm_h)
                if sm_h >= 0.85:
                    headers = headers
                else:
                    return

            if len(headers)==0:
                return

            x_head = headers[0]

            print('Actual ending_key:', ending_key)
            if '\n' in ending_key:
                indxk = int(ending_key.index('\n'))
                print('Local-----index position of /n is --->', indxk)
                ending_key = ending_key[:indxk]
                ending_key = ending_key.rstrip()
                print('Local-----New ending_key value is --->', ending_key)
                ending_key1 = sample_key.split('\n')[-2].rstrip()
                print('Local-----ending_key sliced value is --->', ending_key1)
                if ending_key1 == ending_key:
                    ending_key1 = sample_key.split('\n')[-1].rstrip()
                    print('New Local-----ending_key sliced value is --->', ending_key1)
                ending_key_end_s = ending_key1
                ending_keys = list(filter(lambda x:ending_key.rstrip('*') in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                print(f'Local-----ending_keys matched after searching page: {pno}', ending_keys)
                ending_key1 = list(filter(lambda x:ending_key1.rstrip('*') in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                ending_keys =  [i for i in ending_keys if SM(None,ending_key.rstrip('*'),i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip()).ratio() >0.85]

                ending_key1 =  [i for i in ending_key1 if SM(None,ending_key_end_s.rstrip('*'),i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip()).ratio()]
                print(f'Local-----ending_keys value selected in page: {pno}', ending_keys)
                print(f'Local-----ending_key1 value selected in page: {pno}', ending_key1)
                if len(ending_keys) >1 and len(ending_key1) == 1:
                    ending_keys = [i for i in ending_keys if i['bottom']<=ending_key1[0]['top']<=i['bottom']+10]
                    print(f'Local1-----ending_keys value selected in page: {pno}', ending_keys)
                if len(ending_keys) == 1 and len(ending_key1) == 1:
                    if ending_key1[0]['x0']+0.1 >= ending_keys[0]['x0']:
                        ending_keys[0]['x0'] = sorted(list(set([ending_keys[0]['x0'], ending_key1[0]['x0']])))[0]
                        ending_keys[0]['x1'] = sorted(list(set([ending_keys[0]['x1'], ending_key1[0]['x1']])))[-1]
                        ending_keys[0]['bottom'] = ending_key1[0]['bottom']
                        ending_keys[0]['text'] = ending_keys[0]['text'] + ' ' +ending_key1[0]['text']
                        ending_keys[0]['text'] = ending_keys[0]['text'].lower().replace('  ', ' ').rstrip()
                    print(f'Local1-----ending_keys value selected in page: {pno}', ending_keys)


            else:    
                ending_keys = list(filter(lambda x:ending_key in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                print(f'ending_keys matched after searching page: {pno}', ending_keys)
                ending_keys =  [i for i in ending_keys if ending_key.rstrip() == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip()]
                print(f'ending_keys value selected in page: {pno}', ending_keys)
                if len(ending_keys) > 1:
                    ending_keys = [i for i in ending_keys if headers[0]['x0'] <= i['x0'] <= headers[0]['x1']]
                    if ending_keys:
                        ending_keys = [ending_keys[0]]

        #             if len(ending_keys) > 1:
        #                 ending_keys = [i for i in ending_keys if i['bottom']== ending_keys_bmax]
                if len(ending_keys) == 0:
                    sample_key_split = sample_key.split(' ')
                    print('ending_key_end value is', sample_key_split[-1])
                    ending_keys = list(filter(lambda x:sample_key_split[0] in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    ending_keys_end = list(filter(lambda x:sample_key_split[-1] in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    if ending_keys == ending_keys_end:
                        ending_keys = [i for i in ending_keys if i['x0']>=x_head['x0']-1 or i['x0']<x_head['x1']]
                        if len(ending_keys) > 1:
                            ending_keys_bmax = sorted([i['x0'] for i in ending_keys])[-1]
                            ending_keys = [i for i in ending_keys if i['bottom']== ending_keys_bmax]
                    if len(ending_keys)>= 1 and len(ending_keys_end)>= 1:
                        for est in ending_keys:
                            for i in range(len(ending_keys_end)):
                                if ending_keys_end[i]['top'] == est['top'] and ending_keys_end[i]['bottom'] == est['bottom']:
                                    est['text'] = est['text']+' '+ending_keys_end[i]['text']
                                    est['text'] = est['text'].replace('  ',' ').rstrip()
                                    est['x1'] = ending_keys_end[i]['x1']
                                    ending_keys = [est]
                                elif ending_keys_end[i]['top'] < est['bottom']+20:
                                    est['text'] = est['text']+' '+ending_keys_end[i]['text']
                                    est['text'] = est['text'].replace('  ',' ').rstrip()
                                    est['x1'] = ending_keys_end[i]['x1']
                                    est['bottom'] = ending_keys_end[i]['bottom']
                                    ending_keys = [est]
                    print(f'because actual ending_keys are not matched after searching page newmatch: {pno}', ending_keys)

                    if len(ending_keys) > 1:
                        ending_keys = [i for i in ending_keys if i['x0'] <= headers[0]['x1']]
                        ending_keys_bmax = sorted([i['x0'] for i in ending_keys])[-1]
                        if len(ending_keys) > 1:
                            ending_keys = [i for i in ending_keys if i['bottom']== ending_keys_bmax]
            print('ending_keys before sm is:', ending_keys)
            if ending_keys:
                sm_k = SM(None,f'{sample_key}',f"{ending_keys[0]['text'].lower()}").ratio()
                print('smk value is:', sm_k)
                if sm_k >= 0.85:
                    ending_keys = ending_keys
                else:
                    return
            print('ending_keys final selected value is:', ending_keys)
            if len(ending_keys) == 0:
                return
            x_ending_key = ending_keys[0]
            print('Actual ending_column:', ending_column)
            if '\n' in ending_column:
                indxc = int(ending_column.index('\n'))
                print('index position of /n is --->', indxc)
                ending_column = ending_column[:indxc]
                ending_column = ending_column.rstrip()
                print('New ending_column value is --->', ending_column)
                ending_column1 = sample_column[indxc+1:]
                print('Local-----ending_column sliced value is --->', ending_column1)
                ending_columns = list(filter(lambda x:ending_column.rstrip('*') in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                print(f'Local-----ending_columns matched after searching page: {pno}', ending_columns)
                ending_columns =  [i for i in ending_columns if ending_column.rstrip('*') == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip()]
                print(f'Local-----ending_columns value selected in page: {pno}', ending_columns)
                ending_columns1 = list(filter(lambda x:ending_column1.rstrip('*') in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                print(f'Local-----ending_column1 matched after searching page: {pno}', ending_columns1)
                ending_columns1 =  [i for i in ending_columns1 if ending_column1.rstrip('*') == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip()]
                print(f'Local-----ending_columnns1 value selected in page: {pno}', ending_columns1)
                if len(ending_columns) == 0 and len(ending_columns1) >0:
                    column_s = ending_column.replace(' ','')
                    ending_columns = list(filter(lambda x:column_s.rstrip('*') in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                    print('ending_columns after removing space:', ending_columns)
                    ending_columns =  [i for i in ending_columns if column_s.rstrip('*') == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip()]
                    print('ending_columns comparision value:', ending_columns)
                    if len(ending_columns) > 1:
                        loop_cms = []
                        for i in ending_columns:
                            for j in ending_columns1:
                                if i['bottom'] <= j['bottom'] <= i['bottom']+20:
                                    i['x0'] = sorted(list(set([i['x0'],j['x0']])))[0]+2
                                    i['x1'] = sorted(list(set([i['x1'],j['x1']])))[-1]
                                    i['top'] = sorted(list(set([i['top'],j['top']])))[0]
                                    i['bottom'] = sorted(list(set([i['bottom'],j['bottom']])))[-1]
                                    loop_cms.append(i)
                        print('loop_cms value is:', loop_cms)
                        if len(loop_cms) == 1:
                            ending_columns = loop_cms
                        elif len(loop_cms) > 1:
                            ending_columns = [i for i in ending_columns if x_head['bottom']<=i['top']<=x_head['bottom']+20]
                            print(f'ending_columns after removing space and selection in page: {pno}', ending_columns)                            
                if len(ending_columns) == 0 and '\n' in ending_column1:
                    indxc1 = int(ending_column1.index('\n'))
                    ending_column1 = ending_column1[:indxc1]
                    ending_column1 = ending_column1.rstrip()
                    ending_column = ending_column+' '+ending_column1
                    ending_columns = list(filter(lambda x:ending_column.rstrip('*') in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))    
                    print(f'Local1-----ending_columns matched after searching page: {pno}', ending_columns)
                    ending_columns =  [i for i in ending_columns if ending_column.rstrip('*') == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip('*').rstrip()]
                    print(f'Local1-----ending_columns value selected in page: {pno}', ending_columns)
                if len(ending_columns) == 0:
                    sample_column_n = sample_column.replace('\n', ' ')
                    sample_column_split = sample_column_n.split(' ')
                    print('ending_column_end value is', sample_column_split[-1])
                    ending_columns = list(filter(lambda x:sample_column_split[0] in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    ending_columns_end = list(filter(lambda x:sample_column_split[-1] in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    print(f'because actual ending_columns are not matched after searching page new match: {pno}', ending_columns)
                    print('Ending_columns_end values/value is:', ending_columns_end)
                    if len(ending_columns)>1:
                        ending_columns = [i for i in ending_columns if i['bottom'] > x_ending_key['top'] and i['bottom'] < x_head['bottom']]
                        if len(ending_columns_end)>1:
                            for i in ending_columns_end:
                                for j in ending_columns:
                                    if i['top'] - j['top'] <=10 and i['doctop'] - j['doctop'] <=10 and i['bottom'] - j['bottom'] <= 10:
                                        ending_columns_end = [i]
                    print('ending_columns_end value after condition is:', ending_columns_end)
                    if len(ending_columns)>=1 and len(ending_columns_end)==1:
                        ending_columns = list(filter(lambda x:ending_columns.rstrip() in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                        ending_columns = [i for i in ending_columns if ending_columns_end[0]['top'] - i['top'] <= 10]
                        ending_columns = [i]
                        print('ending_columns value in secondcode:',ending_columns)
                        for end_st in ending_columns:
                            for end_jn in ending_columns_end:
                                print('\nsecond condition satisfied')
                                if end_st['x1'] <= end_jn['x1']:
                                    end_st['x1'] = end_jn['x1']
                                    ending_columns = [end_st]
                                else:
                                    ending_columns = [end_st]

                        print(f'ending_columns matched after head start and end:{pno}', ending_columns)
            else:
                print('actual ending_column:', ending_column)
                ending_columns = list(filter(lambda x:ending_column in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                print(f'ending_columns matched after searching page: {pno}', ending_columns)
                if len(ending_columns)==0:
                    column_sample = sample_column.replace(' ','')
                    ending_columnsn = list(filter(lambda x:column_sample in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                    ending_columnsn= [i for i in ending_columnsn if column_sample == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip() ]
                    ending_columns = [i for i in ending_columnsn if SM(None, column_sample,i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip()).ratio() > 0.9]
                    print(f'ending_columns matched after searching without space in page: {pno}', ending_columns)
        #         ending_columns =  [i for i in ending_columns if ending_column == i['text'].replace('  ', ' ').replace(' (', '(').lower().rstrip() ]
                print(f'ending_columns selected in page: {pno}', ending_columns)

                if len(ending_columns) == 0:
                    sample_column_split = sample_column.split(' ')
                    print('sample_column_split value is:', sample_column_split)
                    print('ending_column_end value is', sample_column_split[-1])
                    ending_columns = list(filter(lambda x:sample_column_split[0] in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    ending_columns_end = list(filter(lambda x:sample_column_split[-1] in x['text'].lower().replace(' (', '(').replace('  ', ' ').rstrip(),words_list))
                    print(f'because actual ending_columns are not matched after searching page new match: {pno}', ending_columns)
                    print('Ending_columns_end values/value is:', ending_columns_end)
                    if len(ending_columns) == 1 and len(ending_columns_end) ==1:
                        print('equal condition satisfied')
                        ec=ending_columns
                        ecd=ending_columns_end
                        ec[0]['text'] = ending_column
                        ec[0]['x1'] = ecd[0]['x1']
                        ec[0]['bottom'] = ecd[0]['bottom']
                        ending_columns = ec
                        print('ending_columns', ending_columns)
                    if len(ending_columns) > 1 and len(ending_columns_end) >1:
                        print('trying sm condition')
                        ending_columns = [i for i in ending_columns if SM(None,f'{ending_column}',f"{i['text']}").ratio() > 0.5]
                        print('ending_column value after sm:', ending_columns)
                        ending_columns= [i for i in ending_columns if i['top']-headers[0]['bottom'] <12]
                        ending_columns_end = [i for i in ending_columns_end if i['top']-headers[0]['bottom'] <12]
                        print('headers condition')
                        print('ending column value', ending_columns)
                        print('end value:', ending_columns_end)
                        if len(ending_columns) == 1:
                            if ending_columns == ending_columns_end:
                                ending_columns_end = ending_columns
                                if len(ending_columns_end) >1:
                                    for i in ending_columns_end:  
                                        if i['x0'] - ending_columns[0]['x1'] < 6:
                                            print('condition satisfied')
                                            ending_columns[0]['text'] =ending_columns[0]['text'] +' '+ i['text']
                                            ending_columns[0]['x1'] = sorted(set([i['x1'],ending_columns[0]['x1']]))[-1]
                                            ending_columns[0]['bottom'] = sorted(set([i['bottom'], ending_columns[0]['bottom']]))[-1]
                                            print('ending_columns value is:', ending_columns)

                    if len(ending_columns)>1:
                        ending_columns = [i for i in ending_columns if i['bottom'] > x_ending_key['top'] and i['top'] < x_head['bottom']]
                        if len(ending_columns_end)>1:
                            for i in ending_columns_end:
                                for j in ending_columns:
                                    if i['top'] - j['top'] <=10 and i['doctop'] - j['doctop'] <=10 and i['bottom'] - j['bottom'] <= 10:
                                        ending_columns_end = [i]
                    print('ending_columns_end value after condition is:', ending_columns_end)
                    if len(ending_columns)>1 and len(ending_columns_end)==1:
                        ending_columns = list(filter(lambda x:ending_columns.rstrip() in x['text'].replace('  ', ' ').replace(' (', '(').lower(),words_list))
                        ending_columns = [i for i in ending_columns if ending_columns_end[0]['top'] - i['top'] <= 10]
        #                 ending_columns = [i]
        #                 print('ending_columns value in secondcode:',ending_columns)
                        for end_st in ending_columns:
                            for end_jn in ending_columns_end:
                                print('\nsecond condition satisfied')
                                if end_st['x1'] <= end_jn['x1']:
                                    end_st['x1'] = end_jn['x1']
                                    ending_columns = [end_st]
                                else:
                                    ending_columns = [end_st]

                        print(f'ending_columns matched after head start and end:{pno}', ending_columns)

            print('ending_columns before sm:', ending_columns)
            if ending_columns:
                sm_c = SM(None, f'{ending_column}',f'{ending_columns[0]["text"].lower()}').ratio()
                print('smcvalue is:', sm_c)
                if sm_c>= 0.85:
                    ending_columns = ending_columns
                else:
                    return
            print('ending_c final selected value is:', ending_columns)

            if len(ending_columns) == 0:
                return 

            ending_pts = [i for i in ending_columns if i['x0']>=x_head['x0']]
            print(f'ending_pts in page: {pno}', ending_pts)
            if len(ending_pts) == 0:
                return 
            ending_x_min = [i['x0'] for i in ending_pts]
            print(pno, ending_x_min)
            ending_x_min = sorted(list(set(ending_x_min)))[0]
            print(pno, ending_x_min)
            min_x0 = ending_x_min
            print(min_x0)
            pt = None
            for point in ending_pts:
                if point['x0'] == min_x0:
                    if pt is None:
                        pt = point
                    elif pt['top']>point['top']:
                        pt = point
            header = headers[0]
            ending_keys=ending_keys
            ending_columns = ending_columns[0]
        #     bbox=(header['x0'],header['top'],pt['x1'],ending_keys['bottom']+2)
            bbox_kv = (pt['x0']-14.9,ending_keys[0]['top']-1,pt['x1']+7,ending_keys[0]['bottom']+4)
        #     bbox=(pt['x0'],ending_keys['top'],pt['x1'],ending_keys['bottom']+2)
        #     bbox_tb = (header['x0'],header['top'],pt['x1'],ending_keys['bottom']+2)
        #     words_list_filtered=[word for word in words_list if word['x0']>=bbox[0] and word['x1']<=bbox[2] and word['top']>=bbox[1] and word['bottom']<=bbox[3]]
            return header,ending_keys,pt,bbox_kv

        def key_down(pdfkey, text, pg_index, header = 'nan'):
            print('input header and pdfkey values are:', header,',', pdfkey)
            text_process = handle_multipleline_kv(text)
            words_list = handle_multiple_row2_kv(text_process)
            if '\n' in pdfkey:
                two_value_condition = 1
                _endk = pdfkey.split('\n')
                endk_start = _endk[0]
                endk_end = _endk[-1]
                _endk_start = list(filter(lambda x: cleanse_key(endk_start) in cleanse_key(x['text']), words_list))
                print('_endk_start value is/are:',_endk_start)
                _endk_start = [i for i in _endk_start if cleanse_key(endk_start) == cleanse_key(i['text'])]
                print('_endk_start match value is:',_endk_start)
                _endk_end = list(filter(lambda x: cleanse_key(endk_end) in cleanse_key(x['text']), words_list))
                print('_endk_end value is/are:',_endk_end)
                _endk_end = [i for i in _endk_end if cleanse_key(endk_end) == cleanse_key(i['text'])]
                print('_endk_end match value is:',_endk_end)
                key_match = multi_single(_endk_start, _endk_end)
                print('key_match multi single value is:', key_match)
            else:
                two_value_condition = 0
                pdfkey = pdfkey
                pdfkey = cleanse_key(pdfkey)
                key_match = list(filter(lambda x: pdfkey in cleanse_key(x['text']), words_list))
                print(f'key_search values are in page:{pg_index+1}', key_match)
                key_match = [i for i in key_match if pdfkey == cleanse_key(i['text'])]
                print(f'key_matches are in page:{pg_index+1}', key_match)
            if len(key_match) > 0 and header != 'nan':
                header_ = header_kv(header, text, pg_index)
                if header_ != None:
                    key_match = [i for i in key_match if i['top'] > header_['top']]
                else:
                    return
            if len(key_match) > 0:
                key_match = key_match[0]
            else:
                return
            print(f'final key_match value is in page:{pg_index+1}', key_match)

            key_down = [i for i in words_list if i['top'] > key_match['bottom']
                        and key_match['x0'] -25 < i['x0'] < key_match['x1']+55]
            if len(key_down) > 1:
                diff_val = abs(key_down[0]['bottom'] - key_down[1]['top'])
                print('diff_val is:', diff_val)
                if two_value_condition == 1 and diff_val < 4.1:
                    key_down = key_down[0]['text'] + ' \n' + key_down[1]['text']
                    print('two value condition satisfied:', key_down)
                elif two_value_condition == 0 and diff_val < 2:
                    key_down = key_down[0]['text'] + ' \n' + key_down[1]['text']
                    print('two value condition satisfied:', key_down)
                else:
                    key_down = key_down[0]['text']
            elif len(key_down) == 0:
                key_down = [i for i in words_list if i['top'] > key_match['bottom']
                        and key_match['x0'] -25 < i['x0'] < key_match['x1']+55]
                print('key_down value is:', key_down)
                key_down_ = [i for i in key_down if i['x1'] > key_match['x0']]
                print('key_down_ value is:', key_down_)
                if len(key_down_) > 0:
                    key_down = key_down_[0]['text']
                else:
                    key_down = 'NaN'

            else:
                key_down = key_down[0]['text']
            print(f'final key_down value is in page:{pg_index+1}', key_down)
            return key_down

        def crop_word_match(act_wrd,text,key_found):
            key_x0 = key_found['x0']-25
            print('key_x0 value is:', key_x0)
            key_x1 = key_found['x1']+120
            print('key_x1 value is:', key_x1)
            text1_ = handle_multipleline_kv(text)
            text2_ = handle_multiple_row2_kv(text1_)
            matching = [i for i in text2_ if i['top'] == act_wrd['top'] and i['bottom'] == act_wrd['bottom']]
            print('single_word mathcing value:', matching)
            matched = [i for i in matching if key_x0 <= i['x0'] <= key_x1 and key_x0 <= i['x1'] <= key_x1 ]
            print('single_word matched with key_condition:', matched)
            if len(matched) == 0:
                print('matched value is zero')
                return {'text':'NaN'}
            else:
                print('matched value:', matched[0])
            return matched[0]

        def crop_value(bbox,text, key_found, pg_index):
            crop = pdf_ob.pages[pg_index].within_bbox(bbox, relative=False)
        #     crop = pdf_ob.pages[pg_index].crop(bbox)
            print('crop value is:', crop)
            crop_words_m = crop.extract_words(x_tolerance=3, y_tolerance=1,keep_blank_chars=True)
            crop_words = mean_creator(crop_words_m)
            print('crop words are:', crop_words)

            if len(crop_words) == 0:
                match = 'NaN'
            elif len(crop_words) >2:
                sample_crop_words = crop_words
                crop_words = crop_words[0]
                print('crop_words greater than 2 value is:', crop_words)
                matched = crop_word_match(crop_words, text, key_found)
                if matched == None:
        #             crop_words1 = sorted(sample_crop_words, key = lambda i: (i['x0'],i['bottom']))
                    crop_words1 = sorted(sample_crop_words, key = lambda i: i['x0'])
                    print('crop_words with x0 condition is:', crop_words1)
                    crop_words2 = handle_multipleline_kv(crop_words1)
                    print('crop_words with multiline condition is:', crop_words2)
                    crop_words = handle_multiple_row2_kv(crop_words2)
                    crop_words = crop_words[0]
                    print('crop_words greater than 2 value is:', crop_words)
                    matched = crop_word_match(crop_words, text, key_found)
                match = matched['text']
                print('final selected word is:', match)
            elif len(crop_words) == 1:
                print('single crop word condition')
                cw = crop_words[0]
                print('act_wrd value is:', cw)
                matched = crop_word_match(cw, text, key_found)
                match = matched['text']
                print('final selected word is:', match)
            elif len(crop_words) == 2 and k_down_2 == 0:
                print('double_single crop word condition')
                cw = crop_words[0]
                print('act_wrd value is:', cw)
                matched = crop_word_match(cw, text, key_found)
                match = matched['text']
                print('final selected word is:', match)
            elif len(crop_words) == 2 and k_down_2 == 1:
                print('double crop word condition')
                cw1 = crop_words[0]
                print('cw1 value is:', cw1)
                cw2 = crop_words[1]
                print('cw2 value is', cw2)
                if cw1['x0'] == cw2['x0'] or cw1['x1'] == cw2['x1'] or cw1['x1'] <cw2['x1']<cw1['x1']+0.05 or cw1['x1']-0.1 <= cw2['x1'] <= cw1['x1']:
                    print('double crop condition1 verified')
                    diff_val = cw2['top'] - cw1['bottom']
                    print('diff_value is', diff_val)
                    if diff_val <= 4:
                        print('double crop condition satisfied croping...')
                        cw1_match = crop_word_match(cw1, text, key_found)
                        cw2_match = crop_word_match(cw2, text, key_found)
                        match = cw1_match['text']+'\n'+cw2_match['text']
                        print('double crop value is:', match)
                else:
                    print('double crop condition1 not satisfied')
                    cw1_match = crop_word_match(cw1, text, key_found)
                    match = cw1_match['text']
                    print('single croping, value is:', match)

            print('final match value is:', match)

            return match

        def key_right(pdfkey,text, pg_index, header = 'nan'):
            words_list = text_process(text)
            try:
                if '\n' in pdfkey:
                    key_split = pdfkey.split('\n')
                    key1 = cleanse_key(key_split[0])
                    key2 = cleanse_key(key_split[1])
                    key1_search = list(filter(lambda x: key1 in cleanse_key(x['text']), words_list))
                    key1_match = [i for i in key1_search if cleanse_key(i['text'])== key1]
                    print(f'key1 match value is in page:{pg_index+1}', key1_match)
                    key2_search = list(filter(lambda x: key2 in cleanse_key(x['text']), words_list))
                    key2_match = [i for i in key2_search if cleanse_key(i['text']) == key2]
                    print(f'key2 match value is in page:{pg_index+1}', key2_match)

                    if len(key1_match) == 0 and len(key2_match) == 0:
                        key_new = key1+key2
                        key_new = cleanse_key(key_new)
                        key_match = list(filter(lambda x: key_new in cleanse_key(x['text']), words_list))
                        key_match = [i for i in key_match if key_new == cleanse_key(i['text'])]
                        print(f'key combined match value is in page:{pg_index+1}', key_match)

                    elif len(key1_match) == 0 or len(key2_match) == 0:
                        if key1_match:
                            key_match = key1_match
                        else:
                            key_match = key2_match
                        print(f'single key_match condition in page:{pg_index+1}', key_match)

                    else:
                        key_match = multi_single(key1_match, key2_match)
                        print(f'double line key_match value is in page:{pg_index+1}', key_match)

                else:
                    key = cleanse_key(pdfkey)
                    key_search = list(filter(lambda x: key in cleanse_key(x['text']), words_list))
                    print(f'key_search value in page:{pg_index+1}',key_search)
                    key_match = [i for i in key_search if key == cleanse_key(i['text'])]
                    print(f'key_match value is in page:{pg_index+1}', key_match)
                if len(key_match) == 0:
                    print('key_match value is zero')
                    return
                if len(key_match) > 1 and header != 'nan':
                    header_ = header_kv(header, text, pg_index)
                    if header_ != None:
                        key_match = [i for i in key_match if i['bottom'] > header_['top']]
                    else:
                        return
                if len(key_match) > 0:
                    key_match = [key_match[0]]
                print(f'\nfinal key_match value is in page:{pg_index+1}', key_match)
                key_match = key_match[0]
                key_right = [i for i in words_list if i['x0'] > key_match['x0'] and
                            i['top'] > key_match['top']-10 and i['bottom'] < key_match['bottom']+10]
                print('key_right value is:', key_right)
                if len(key_right) == 0:
                    return
                elif len(key_right) > 1:
                    x0_min = [i['x0'] for i in key_right]
                    x0_min = np.min(x0_min)
                    print('x0_min value is:', x0_min)
                    key_right = [i for i in key_right if i['x0'] == x0_min]
                    print('key_right with min x0 value is:', key_right)
                    key_right = key_right[0]['text']
                else:
                    key_right = key_right[0]['text']
                print(f'final key_right value is in page:{pg_index+1}', key_right)
            except IndexError as err:
                print('key_right function issue')
                print(err)
            return key_right
        def header_sub1(header, header_sub, key, column, text, pg_index):
            print('key with Account number searching started')
            page_number = pg_index + 1
            print('actual header value is:', header)
            print('subheader value is:', header_sub)
            header_s1 = header_kv(header, text,pg_index)
            if header_s1 == None:
                return
            text_process = handle_multipleline_kv(text)
            words_list = handle_multiple_row2_kv(text_process)
            header_s2 = list(filter(lambda x: cleanse_key(header_sub) in 
                                cleanse_key(x['text']), words_list))
            print('sub header search value is:', header_s2)
            if len(header_s2) != 0:
                key_value_hs = ending_key_kv(key, text, header_s1, pg_index)
                print(key_value_hs)
                if key_value_hs == None:
                    return
                column_value_hs = ending_column_kv(column,text, header_s1, key_value_hs, pg_index)
                if column_value_hs == None:
                    return
                print(column_value_hs)
            else:
                return

            box2_kv = [column_value_hs['x0']-14.9, key_value_hs[0]['top']-1, column_value_hs['x1']+7, key_value_hs[0]['bottom']+4]
            print('bbox value is:', box2_kv)
            value = crop_value(box2_kv, text, column_value_hs, pg_index)
            print('\n')
            print('key-value found is:',page_number, value)
            print('\n')
            return value

        def crop_value_2(bbox,text, key_found, pg_index):
            crop = pdf_ob.pages[pg_index].within_bbox(bbox, relative=False)
        #     crop = pdf_ob.pages[pg_index].crop(bbox)
            print('crop value is:', crop)
            crop_words_m = crop.extract_words(x_tolerance=3, y_tolerance=3,keep_blank_chars=True)
            crop_words = mean_creator(crop_words_m)
            print('crop words are:', crop_words)

            if len(crop_words) == 0:
                match = 'NaN'
            elif len(crop_words) >2:
                sample_crop_words = crop_words
                crop_words = crop_words[0]
                print('crop_words greater than 2 value is:', crop_words)
                matched = crop_word_match2(crop_words, text, key_found)
                if matched == None:
        #             crop_words1 = sorted(sample_crop_words, key = lambda i: (i['x0'],i['bottom']))
                    crop_words1 = sorted(sample_crop_words, key = lambda i: i['x0'])
                    print('crop_words with x0 condition is:', crop_words1)
                    crop_words2 = handle_multipleline_kv(crop_words1)
                    print('crop_words with multiline condition is:', crop_words2)
                    crop_words = handle_multiple_row2_kv(crop_words2)
                    crop_words = crop_words[0]
                    print('crop_words greater than 2 value is:', crop_words)
                    matched = crop_word_match2(crop_words, text, key_found)
                if matched == None:
                    return 'NaN'
                match = matched['text']
                print('final selected word is:', match)
            elif len(crop_words) == 1:
                print('single crop word condition')
                cw = crop_words[0]
                print('act_wrd value is:', cw)
                matched = crop_word_match2(cw, text, key_found)
                match = matched['text']
                print('final selected word is:', match)
            elif len(crop_words) == 2 and k_down_2 == 0:
                print('double_single crop word condition')
                cw = crop_words[0]
                print('act_wrd value is:', cw)
                matched = crop_word_match2(cw, text, key_found)
                match = matched['text']
                print('final selected word is:', match)
            elif len(crop_words) == 2 and k_down_2 == 1:
                print('double crop word condition')
                cw1 = crop_words[0]
                print('cw1 value is:', cw1)
                cw2 = crop_words[1]
                print('cw2 value is', cw2)
                if cw1['x0'] == cw2['x0'] or cw1['x1'] == cw2['x1'] or cw1['x1'] <cw2['x1']<cw1['x1']+0.05 or cw1['x1']-0.1 <= cw2['x1'] <= cw1['x1']:
                    print('double crop condition1 verified')
                    diff_val = cw2['top'] - cw1['bottom']
                    print('diff_value is', diff_val)
                    if diff_val <= 4:
                        print('double crop condition satisfied croping...')
                        cw1_match = crop_word_match2(cw1, text, key_found)
                        cw2_match = crop_word_match2(cw2, text, key_found)
                        match = cw1_match['text']+'\n'+cw2_match['text']
                        print('double crop value is:', match)
                else:
                    print('double crop condition1 not satisfied')
                    cw1_match = crop_word_match2(cw1, text, key_found)
                    match = cw1_match['text']
                    print('single croping, value is:', match)

            print('final match value is:', match)

            return match


        def text_process(text):
            text1 = list(text)
            text1_ = handle_multipleline_kv(text1)
            text2_ = handle_multiple_row2_kv(text1_)
            return text2_

        def word_match_multi_line(text, wrd):
            print('start word_match_multi_line function')
            text1 = text_process(text)
            wrd = list(wrd)
            print('given wrd is:', wrd)
            wrd1 = wrd[0]
            match = [i for i in text1 if 0<= i['top']-wrd1['bottom'] < 4.2]
            if len(match) > 1:
                match = [match[0]]
            else:
                match = match
            print('match value is:', match)
            if len(match) == 1:
                print('in if condition')
                matched1 = multi_single(wrd, match)
                matched2 = word_match_multi_line(text, matched1)
            else:
                matched2 = wrd
            return matched2

        def crop_word_match2(act_wrd,text,key_found):
            key_x0 = key_found['x0']-25
            print('key_x0 value is:', key_x0)
            key_x1 = key_found['x1']+120
            print('key_x1 value is:', key_x1)
            text1 = text_process(text)
            matching = [i for i in text1 if i['top'] == act_wrd['top'] or i['bottom'] == act_wrd['bottom']]
            print('single_word mathcing value:', matching)
            matched = [i for i in matching if key_x0 <= i['x0'] <= key_x1 and key_x0 <= i['x1'] <= key_x1 ]
            print('single_word matched with key_condition:', matched)
            if len(matched) > 0:
                match = word_match_multi_line(text, matched)
                return match[0]
            else:
                return 'NaN'

        def column_sub(header, key, column, column_sub,text, pg_index):
            page_number = pg_index + 1
            header_v = header_kv(header, text, pg_index)
            print('header_v found value is:', header_v)
            if header_v == None:
                print('\n')
                print('No header match found')
                return
                print('\n')
            print('\n column sub value is:', column_sub)
            column_sub_v = header_kv(column_sub, text, pg_index)
            print('column_sub value is:', column_sub_v)
            if column_sub_v == None:
                print('\n')
                print('column_sub value is zero')
                return
                print('\n')
            header_v['top'] = column_sub_v['top']
            header_v['bottom'] = column_sub_v['bottom']
            print('New header value is:', header_v)

            key_v = ending_key_kv(key, text, header_v, pg_index)
            print('key value is:', key_v)
            # column_sub_v = ending_column_kv(column_sub, text, header_v, key_v, pg_index)

            words_list = text_process(text)
            column_v_wrds = [i for i in words_list if i['bottom'] >= column_sub_v['top']-10]
        #     column_v_wrds = [i for i in column_v_wrds if i['bottom'] <= column_sub_v['bottom']+30]
            print('column_v_wrds are:', column_v_wrds)
            column_v = [i for i in column_v_wrds if cleanse_key(i['text']) == cleanse_key(column)]
            if len(column_v) == 0:
                print('column_sub no value matched')
                return 
            if len(column_v) > 1:
                column_v = column_v[0] # first dict element in list
            else:
                column_v = column_v[0] # dict element in list
            print('column_v value is:', column_v)

            values = value_crop_extractor(header_v, key_v, column_v,text,pg_index)
            print('croped value is:', values)
            return values

        #     box2_kv = [column_v['x0'], key_v['top']-1, column_v['x1']+7, key_v['bottom']+4]
        #     print('bbox value is:', box2_kv)
        #     value = crop_value_2(box2_kv, text, column_v, pg_index)
        #     print('\n')
        #     print('key-value found is:',page_number, value)
        #     print('\n')
        #     print('value is:', value)
        #     return value

        def multi_key_extractor(multi_values):
            # with top and bottom condition
            wrd_temp = []
            for idx in range(len(multi_values)):
                if len(wrd_temp) == 0:
                    wrd_temp.append(multi_values[idx])
                else:
                    diff_val = abs(wrd_temp[len(wrd_temp)-1]['bottom'] - multi_values[idx]['top'])
                    if diff_val <=27:
                        wrd_temp.append(multi_values[idx])
        #     print('temporary words are:', wrd_temp)

            # with x0 and x1 condition
            wrds_x = list(multi_values)
            wrds_x = [i for i in wrds_x if wrds_x[0]['x0']-1 <= i['x0'] <=wrds_x[0]['x0']+1 and wrds_x[0]['x1']-1 <= i['x1'] <= wrds_x[0]['x1']+1]
            wrds_x = sorted(wrds_x, key = lambda i: i['top'])
            print('words with boundary x conditions are:', wrds_x)

            if len(wrd_temp) > len(wrds_x):
                print('length of wrd_temp greater than wrds_x')
                final_wrd_list = wrd_temp
            elif len(wrd_temp) < len(wrds_x):
                print('length of wrds_x greater than wrd_temp')
                tmp_wrd = []
                for wrd_idx in range(len(wrds_x)):
                    if len(tmp_wrd) == 0:
                        tmp_wrd.append(wrds_x[wrd_idx])
                    else:
                        diff_val = abs(tmp_wrd[len(tmp_wrd)-1]['bottom']-wrds_x[wrd_idx]['top'])
                        if diff_val < 100:
                            tmp_wrd.append(wrds_x[wrd_idx])
                        else:
                            continue
                print('tmp_wrd values are:', tmp_wrd)
                final_wrd_list = tmp_wrd
            else:
                final_wrd_list = wrd_temp
                print('length are sampe wrd_temp taken')
            return final_wrd_list

        def mean_creator(lst_wrds):
            result = []
            for wrds in lst_wrds:
                wrds['x_mean'] = int((wrds['x0'] + wrds['x1'])/2)
                wrds['y_mean'] = int((wrds['top'] + wrds['bottom'])/2)
                result.append(wrds)
            # print('mean result is:', result)
            return result   

        def search(wrd, text):
            text1 = list(text)
            text1_ = handle_multipleline_kv(text1)
            text2_ = handle_multiple_row2_kv(text1_)
            search = list(filter(lambda x: cleanse_key(wrd) in cleanse_key(x['text']), text2_))
            return search

        def match_value(crp_wrds, txt_wrds):
            search_v_extr = list(crp_wrds)
            search_v_text = list(txt_wrds)
            match = search_v_extr[0]
        #     print(f'\nsearch_v_ext-{i} value is:', search_v_extr)
            match_v_text = [i for i in search_v_text if match['top']-5 <= i['top'] <= match['top']+5]
            match_v_text = [i for i in match_v_text if match['bottom']-5 <= i['bottom'] <= match['bottom']+5]
            return match_v_text



        def column_wrt_key(key_value, column_v, text, pg_index):
            text1 = list(text)
            wrds_list = text_process(text1)
            column_vs = dict(column_v)
        #         clmn_bk = [i for i in wrds_list if column_vs['top']-5 < i['bottom'] and i['top'] <= key_value[idx]['top']]
            clmn_k = [i for i in wrds_list if column_vs['top']-5 < i['bottom'] and i['bottom'] <= key_value['bottom']]
            clmn_vf = [i for i in clmn_k if column_vs['x0'] > i['x1']]
            clmn_v = [i for i in clmn_k if column_vs['x1'] < i['x0']]
            if len(clmn_v) > 0 and len(clmn_vf) > 0:
                min_x0 = min([i['x0'] for i in clmn_v])
                max_x1 = max([i['x1'] for i in clmn_vf])
                diff_vl_x0 = column_v['x0'] - max_x1
                diff_vl_x1 = min_x0 - column_v['x1']
                clmn_new = column_vs
                if diff_vl_x0 > 0:
                    clmn_new['x0'] = max_x1
                    prev_clmn = [i for i in clmn_vf if i['x1'] == max_x1]
                else:
                    clmn_new['x0'] = column_vs['x0']
                    prev_clmn = ['nan']
                if diff_vl_x1 > 0:
                    clmn_new['x1'] = min_x0
                    nex_clmn = [i for i in clmn_v if i['x0'] == min_x0]
                else:
                    clmn_new['x1'] = column_vs['x1']
                    nex_clmn = ['nan']

                print('nex_clmn value is:', nex_clmn[0])
                print('prev_clmn value is:', prev_clmn[0])
                print('\n')
                print('new column value is:', clmn_new)
                print('\n')
            elif len(clmn_v) > 0 and len(clmn_vf) == 0:
                min_x0 = min([i['x0'] for i in clmn_v])
                diff_vl_x1 = min_x0 - column_v['x1']
                nex_clmn = [i for i in clmn_v if i['x0'] == min_x0]
                clmn_new = column_v
                if diff_vl_x1 > 0:
                    clmn_new['x1'] = min_x0
                    nex_clmn = [i for i in clmn_v if i['x0'] == min_x0]
                else:
                    clmn_new['x1'] = column_vs['x1']
                    nex_clmn = ['nan']
                print('nex_clmn value is:', nex_clmn[0])
                print('min_x0 value is:', min_x0)
                print('new column value is:', clmn_new)

            else:
                clmn_new = column_vs

            return clmn_new

        def multi_cell_check(wordslist1, key_ref1):
            print('key_ref1 value is:', key_ref1)
            key_ref = dict(key_ref1)
            words_temp = []  
            wordslist = list(wordslist1)
            wordslist = sorted(wordslist, key = lambda i: i['top'])
            for i in range(len(wordslist)):
                if len(words_temp) == 0:    
                    words_temp.append(wordslist[i])
                else:
                    diff_val = abs(words_temp[len(words_temp)-1]['bottom'] - wordslist[i]['top']) 
                    if diff_val > 12.5:
                        if wordslist[i]['top'] < key_ref['bottom']:
                            words_temp.clear()
                            words_temp.append(wordslist[i])
                        else:
                            pass
                    else:
                        words_temp.append(wordslist[i]) 
            return words_temp


        def crop_words_pgimage_test(hd,pdf_ob,bbox,text,key_value, pg_index):
            text1 = list(text)
        #     wrd_list = text_process(text1)
            page_im = pdf_ob.pages[pg_index].within_bbox(bbox, relative = False)
            crop_value = page_im.extract_text()
            print('crop_value extracted:', crop_value)
        #     crop_wrds = [i for i in wrd_list if bbox[0] < i['x_mean'] <= bbox[2]]
            crop_wrds = [i for i in pop_lst if bbox[0] < i['x_mean'] <= bbox[2]]
            print('crop_wrds with column_condition:', crop_wrds)
            crop_wrds = [i for i in crop_wrds if bbox[1] < i['y_mean'] <= bbox[3]]
            print('crop_wrds with key_condition:', crop_wrds)
            if len(crop_wrds) > 1:
                crop_wrds = multi_cell_check(crop_wrds, key_value)
            if '\n' in crop_value:
                c_wrds = []
                c_value = ''
                cv_split = crop_value.split('\n')
                print('crop value with /n split condition and length:',len(cv_split),' | ', cv_split)
                crop_wrds1 = list(crop_wrds)
                for indx in range(len(cv_split)):
                    crp_wrds = crop_wrds1
        #             crp_wrds = multi_cell_check(crop_wrds1, key_value)
                    cv_search = [i for i in crp_wrds if cleanse_key(i['text']) == cleanse_key(cv_split[indx])]
                    if len(cv_search) == 0:
                        cv_search = [i for i in crp_wrds if cleanse_key(cv_split[indx]) in cleanse_key(i['text'])]
                    if len(cv_search) >= 1:
                        if indx < len(cv_split) - 1:
                            c_value += cv_search[0]['text'] + '\n'
                            c_wrds.append(cv_search[0])
                            pop_lst.remove(cv_search[0])
                            crop_wrds1.remove(cv_search[0])
                        else:
                            c_value += cv_search[0]['text']
                            c_wrds.append(cv_search[0])
                            pop_lst.remove(cv_search[0])
                            crop_wrds1.remove(cv_search[0])
                    else:
                        continue
                crop_words_vf = sorted(c_wrds, key = lambda i: i['top'])
                if c_wrds == crop_words_vf:
                    print('After sorting condition and crop words are same')
                else:
                    c_value = ''
                    for idx in range(len(crop_words_vf)):
                        if idx < len(crop_words_vf) - 1:
                            c_value += crop_words_vf[idx]['text'] + '\n'
                        else:
                            c_value += crop_words_vf[idx]['text']
            else:
                c_wrds = []
                c_value = ''
                crop_wrds1 = list(crop_wrds)

                for indx in range(len(crop_wrds)):
                    crp_wrds = crop_wrds1
        #             crp_wrds = multi_cell_check(crop_wrds1, key_value)
                    cv_search = [i for i in crp_wrds if cleanse_key(i['text']) in cleanse_key(crop_value)]
                    if len(cv_search) == 0:
                        cv_search = [i for i in crp_wrds if cleanse_key(crop_value) in cleanse_key(i['text'])]
                    if len(cv_search) >= 1:
                        if indx < len(crop_wrds) - 1:
                            c_value += cv_search[0]['text'] + '\n'
                            c_wrds.append(cv_search[0])
                            pop_lst.remove(cv_search[0])
                            crop_wrds1.remove(cv_search[0])
                        else:
                            c_value += cv_search[0]['text']
                            c_wrds.append(cv_search[0])
                            pop_lst.remove(cv_search[0])
                            crop_wrds1.remove(cv_search[0])
                    else:
                        continue
        #     c_wrds_final = multi_cell_check(c_wrds, key_value)
            print('crop value and words with crop condition:', c_value,' | ', c_wrds)

            return c_wrds, c_value

        def value_crop_extractor(header_v, key_value, column_v,text, pg_index):
            values = []
            text_ = list(text)
            wrds_list = text_process(text_)
            global pop_lst
            pop_lst = list(wrds_list)
            for idx in range(len(key_value)):
                print('\n')
                print('\n')
                print('Actual header, key, column values are:', header_v, '\n', key_value[idx], '\n', column_v)
                print('\n')
                print('\n')
                key_value_s = dict(key_value[idx])
                clmn_new = column_wrt_key(key_value[idx], column_v, text, pg_index)
                print('column_new value is:', clmn_new)
                key_v_f = [i for i in wrds_list if header_v['top'] <= i['top'] < key_value[idx]['top']]
                key_v_f = [i for i in key_v_f if i['x0'] < key_value[idx]['x1'] and i['x_mean'] > key_value[idx]['x0']]
                key_v = [i for i in wrds_list if i['y_mean'] > key_value[idx]['bottom']]
                key_v = [i for i in key_v if i['x0']<key_value[idx]['x1']]

                if len(key_v) > 0 and len(key_v_f) > 0:
                    min_top = min([i['top'] for i in key_v])
                    max_bottom = max([i['bottom'] for i in key_v_f])
                    dif_bottom = max_bottom - column_v['bottom']
                    if dif_bottom > 0:
                        ab_ref = [i for i in key_v_f if i['bottom'] == max_bottom]

                    else:
                        max_bottom = column_v['bottom']
                        print('\n')
                        print('column bottom greater than conditional bottom')
                        ab_ref = dict(column_v)
                        print('')
                    bw_key = [i for i in key_v if i['top'] == min_top]
                    print('\n')
                    print('above ref value is:', ab_ref)
                    print('below key value is:', bw_key)
                    print('min_top and max_bottom values are:', min_top,' | ', max_bottom)
                    key_new = key_value_s
                    diff_vl_t = key_value_s['top'] - max_bottom
                    diff_vl_b = min_top - key_value_s['bottom']
        #             diff_vl_k = key_value[idx]['bottom']
        #             diff_val_k = abs(key_v['top'] - key_value[idx]['bottom'])
        #             if 25 < diff_val_k < 30:
                    if diff_vl_t > 0:
                        key_new['top'] = max_bottom
                    else:
                        key_new['top'] = key_value_s['top']
                    if diff_vl_b > 0:
                        key_new['bottom'] = min_top
                    else:
                        key_new['bottom'] = key_value_s['bottom']
                    print('new key value is:', key_new)
        #                 print('new key value is', key_new)
        #             bbox_c = (clmn_new['x0'], key_new['top'], clmn_new['x1'], key_new['bottom'])
        #             else:
        #                 key_new = key_value[idx]
        #                 bbox_c = (clmn_new['x0'], key_new['top'], clmn_new['x1'], key_new['bottom'])
                else:
                    key_new = key_value_s

                clmn_new_k = column_wrt_key(key_new, column_v, text, pg_index)
                print('column new value wrt key is:', clmn_new_k)
        #             bbox_c = (clmn_new['x0'], key_new['top'], clmn_new['x1'], key_new['bottom'])
        #             print('next key not found:', key_new)

        #         bbox_c = (clmn_new['x0'], key_new['top'], clmn_new['x1'], key_new['bottom'])
                bbox = (clmn_new_k['x0'], key_new['top'], clmn_new_k['x1'], key_new['bottom'])

        #         print('bbox value for crop condition is:', bbox_c)
                print('bbox value for box condition is:', bbox)
        #         crop_words, crop_value = crop_words_pgimage(header_v, pdf_ob, bbox, text,key_value[idx], pg_index)
                crop_words, crop_value = crop_words_pgimage_test(header_v, pdf_ob, bbox, text,key_value[idx], pg_index)
        #         match = []
                print('crop_words and crop_value is:', crop_words, crop_value)
                if len(crop_words) == 0:
                    values.append('NaN')     
                elif len(crop_words) >= 1:
                    values.append(crop_value)

            print('all values are:', values)
            return values

        def extract_kv(header,ending_key,ending_column,uniq,ex_type):
            page_kv = []
            for i in page_cnt:
                header = str(header).lower()
                ending_key = str(ending_key).lower()
                ending_column = str(ending_column).lower()
                ex_type = str(ex_type).lower()
                uniq = str(uniq).lower()
                text_m = pdf_ob.pages[i].extract_words(x_tolerance=3, y_tolerance=3, keep_blank_chars=True, use_text_flow=False, horizontal_ltr=True, vertical_ttb=True, extra_attrs=[])
                text = mean_creator(text_m)
                if ex_type == 'key-value-down' :
                    print('\nkey-value-down searching started------')
                    if header == 'nan':
                        print('ending_key value is:', ending_key)
                        key_value_down = key_down(ending_key,text, i)
                        print('key_value_down in extract_kv:', key_value_down)
                    else:
                        print('key_down with header')
                        print('header value is:', header)
                        print('ending_key value is:', ending_key)
                        key_value_down = key_down(ending_key, text, i, header)
                        print('key_value_down with header:', key_value_down)

                    if key_value_down != None:
                        key_d = [header, ending_key, 'NaN', key_value_down.replace('(', '-').rstrip(')'),str(i+1)]
                        page_kv.append(key_d)
                    else:
                        pass

                if ex_type == 'sub_table_extraction':
                    print('\n')
                    print('key extraction using sub column value started')
                    print('\n')
                    values_c = column_sub(header, ending_key, ending_column, uniq, text, i)
                    print('\n')
                    print('column_sub value is:', values_c)
                    print('\n')
                    #             if value_c != None:
        #                 nex_c = [header, ending_key,ending_column,value_c.replace('(', '-').rstrip(')'),str(i+1)]
        #                 page_kv.append(nex_c)
                    if values_c == None:
                        nex_c = [header, ending_key,ending_column, 'NaN',str(i+1)] 
                    else:
                        print('length of values are:', len(values_c))
                        for idx_c in range(len(values_c)):
                            inx_c = [header, ending_key,ending_column, values_c[idx_c].replace('(', '-').rstrip(')').replace('—','__'),str(i+1)]
                            page_kv.append(inx_c)

        #         if ex_type == 'sub_table_extraction':
        #             print('\n')
        #             print('key extraction using sub column value started')
        #             print('\n')
        #             value_c = column_sub(header, ending_key, ending_column, uniq, text, i)
        #             print('\n')
        #             print('column_sub value is:', value_c)
        #             print('\n')
        #             if value_c != None:
        #                 nex_c = [header, ending_key,ending_column,value_c.replace('(', '-').rstrip(')'),str(i+1)]
        #                 page_kv.append(nex_c)
        #             else:
        #                 pass

                if ex_type == 'key-value-right':
                    print('\nkey-value-right searching started------')
                    if header == 'nan':
                        print(f'ending_key value in page:{i+1}',ending_key)
                        key_value_right = key_right(ending_key, text, i)
                        print('key_value_right without header:', key_value_right)
                    else:
                        print('key_right with header')
                        print('header value is:', header)
                        print('ending_key value is:', ending_key)
                        key_value_right = key_right(ending_key, text, i, header)
                        print('key_value_right with header:', key_value_right)
                    if key_value_right != None:
                        key_r = [header, ending_key, 'NaN', key_value_right.replace('(', '-').rstrip(')'),str(i+1)]
                        page_kv.append(key_r)
                    else:
                        pass

                if ex_type == 'key-value':
                    if uniq == 'nan':
                        print('\nkey-value searching started------')
                        out = get_respective_fields(header, ending_key,ending_column,text,i)
                        pdf_ob.pages[i].flush_cache()
                        if out == None:
                            text1_m = pdf_ob.pages[i].extract_words(x_tolerance=4.31, y_tolerance=3, keep_blank_chars=True, use_text_flow=False, horizontal_ltr=True, vertical_ttb=True, extra_attrs=[])
                            text1 = mean_creator(text1_m)
                            global pno
                            pno = i + 1
                            header_2 = header
                            header_2 = header_2.replace('\n(', '(').replace(' (', '(')
                            ending_key2 = ending_key
                            ending_key2 = ending_key2.replace(' (', '(').replace("d_", '')
                            out = get_respective_fields_kv(header_2, ending_key2,ending_column,text1)
                        if out == None:
                            pass

                        elif out is not None:
                            header_v = out[0]
                            key_v = out[1]
                            column_v = out[2]
                            values = value_crop_extractor(header_v, key_v, column_v,text,i)
                            if values == None:
                                inx = [header, ending_key,ending_column, 'NaN',str(i+1)] 
                            else:
                                print('length of values are:', len(values))
                                for idx in range(len(values)):
                                    inx = [header, ending_key,ending_column, values[idx].replace('(', '-').rstrip(')').replace('—','__'),str(i+1)]
                                    page_kv.append(inx)
            #                     box2_kv = out[-1]
            #                     key_found = out[-2]
            #                     print('bbox value is:', box2_kv)
            #                     value = crop_value(box2_kv, text, key_found, i)
            #                     print('\n')
            #                     print('key-value found is:',i+1, value)
            #                     print('\n')
            #                     nex = [header, ending_key,ending_column,value.replace('(', '-').rstrip(')'),str(i+1)]
            #                     page_kv.append(nex)

                    else:
                        print('\nkey-value header_sub searching started------')
                        value = header_sub1(header, uniq, ending_key, ending_column, text, i)
                        print('\n')
                        print('header_sub value is:', value)
                        print('\n')
                        if value != None:
                            nex = [header, ending_key,ending_column,value.replace('(', '-').rstrip(')'),str(i+1)]
                            page_kv.append(nex)
                        else:
                            pass

                        # sub table_category



            return page_kv

        def dataframe(data):
            ls2 = []
            global final_df
            final_df = pd.DataFrame()
            for i in data:
                for j in i:
                    ls2.append(pd.DataFrame(j))

                    # st.write(ls2)

                    final_df = pd.concat(ls2,ignore_index=True)
                    # print('final_df value is', final_df)
                    final_df.index = list(range(1, len(final_df) + 1))


            return final_df    

        def multi_tru(lengths):

            # sx=[]
            global he1
            # global endk
            # global endc
            # fname = upload_file.name.rstrip("pdf").rstrip("PDF").rstrip(".")+str("_output")+str(".xlsx")

            # writer = pd.ExcelWriter(fname,engine='xlsxwriter')
            # st.write(zx)
            # for i in range(len(zx)):
            # he1, endk1,endc1 = zx

            # sx.append(dataframe(extract_tru(he1, endk1,endc1,lengths)))
            # gre = dataframe(extract_tru(he1, endk1,endc1,lengths))
            # dfs1 = sx
            # st.write(type(dfs1))
            # csv = convert_df(dfs)
            # st.write(dfs1)
            # st.write(gre)
            # st.download_button(label="Download data as CSV",data=convert_df(gre),file_name='pdf_output.csv' ,mime='text/csv',)

            # sx[i].to_excel(writer,sheet_name=he1,header=None,index=False)
            # writer.save()

            return 


        def get_points(my_list):
            # output will be x0,y0,x1,y1
            if type(my_list) != list:
                my_list = [my_list]
            x0 = my_list[0]['x0']
            y0 = my_list[0]['y0']
            x1 = my_list[-1]['x1']
            y1 = my_list[-1]['y1']
            return x0, y0, x1, y1

        def find_lexical_sequence(indexes, w_indexes):
            if len(indexes) == 1:
                return w_indexes
            out = []
            s = indexes[0]
            for idx, i in enumerate(indexes[1:]):
                if s - i == -1:
                    out.append(w_indexes[idx + 1])
                else:
                    out = []
                s = i
            if len(out) > 0:
                out = [out[0] - 1] + out
            return out

        def clean_row_points(row_points, word_list, word):
            final_row_points = []
            row_points_key = list(row_points.keys())[0]
            ignore_words = ['']#['and']
            for row in row_points[row_points_key]:
                word_found = word_list[row[-1][0]:row[-1][-1] + 1]
                w_split = [i for i in word.split(" ") if len(i) > 0]
                matched = 0
                unmatched = 0
                for word_x in cleanse_key(" ".join(word_found)).split():
                    word_x = word_x.strip()
                    if word_x in ignore_words or len(word) == 0:
                        continue
                    elif word_x in word:
                        matched += 1
                    else:
                        unmatched += 1
                if matched == len(w_split) and unmatched == 0:
                    final_row_points.append(row)
            row_points[row_points_key] = final_row_points
            return row_points

        def look_for_key_vals(word_list, word, words_with_location,value_split = True):
            if value_split:
                word_split = [i for i in word.split(" ") if len(i) > 0]
            else:
                word_split = [word]
            indexes = []
            w_indexes = []
            w_c = word
            for word_idx, word in enumerate(word_list):
                if word.strip() in word_split:
                    w_indexes.append(word_idx)
                    indexes.append(word_split.index(word.strip()))
                else:
                    for word_x in word_split:
                        if word_x in word:
                            w_indexes.append(word_idx)
                            indexes.append(word_split.index(word_x))
            if len(word_split) != len(set(indexes)):
                return ''
            out = find_lexical_sequence_1(indexes, w_indexes, value_check=True)
            row_points = dict()
            row_points[w_c] = []
            for i in out:
                pts = get_points([words_with_location[i[0]], words_with_location[i[-1]]])
                main_pts = (pts, (i[0], i[-1]))
                row_points[w_c].append(main_pts)
            if len(row_points) > 0:
                # print(row_points)
                row_points = clean_row_points(row_points, word_list, w_c)
            return row_points

        def detect_values_nag(row_points, column_points, word_list, final_words):
            matches = []
            if type(row_points) != dict:
                return matches
            row_match = []
            col_vals = list(column_points.values())[0]
            col_x0 = ([i[0] for i in col_vals])
            sorted_x0 = sorted(col_x0)
            new_col_vals = [col_vals[col_x0.index(i)] for i in sorted_x0]
            row_vals = list(row_points.values())[0]
            word = list(row_points.keys())[0]
            col_vals = new_col_vals
            for row in row_vals:
                row_x0 = row[0][0]
                row_x1 = row[0][2]
                row_y0 = row[0][1]
                row_y1 = row[0][-1]
                start = row[-1][-1] + 1
                for col in col_vals:
                    if row[-1] not in row_match:
                        col_x0 = col[0][0]
                        col_x1 = col[0][2]
                        min_x = min(col_x0, col_x1)
                        max_x = max(col_x0, col_x1)
                        for word_index, word_element in enumerate(final_words):
                            word_x0 = word_element['x0']
                            word_x1 = word_element['x1']
                            word_y0 = word_element['y0']
                            word_y1 = word_element['y1']
                            word_center = word_x0 + ((word_x1 - word_x0) / 2)
                            word_y_center = word_y0 + ((word_y1 - word_y0) / 2)
                            append_idx = word_index

                            if append_idx in matches:
                                continue
                            elif min_x < word_center < max_x and row_y0 < word_y_center < row_y1:
                                #                         print("x_center:",word_center,"y_center:",word_y_center)
                                #                         print("x_boundaries:",col_x0,col_x1)
                                #                         print("y_boundaries:",row_y0,row_y1)
                                matches.append(word_index)
                                # print(word_element)
                                row_match.append(row[-1])
                                break
                    else:
                        break
            # print(matches)
            # print(row_match)
            return matches

        def detect_values_1(row_points, column_points, word_list, final_words):
            matches = []
            if type(row_points) != dict:
                return matches
            row_match = []
            col_vals = list(column_points.values())[0]
            col_x0 = ([i[0] for i in col_vals])
            sorted_x0 = sorted(col_x0)
            new_col_vals = [col_vals[col_x0.index(i)] for i in sorted_x0]
            row_vals = list(row_points.values())[0]
            word = list(row_points.keys())[0]
            col_vals = new_col_vals
            for row in row_vals:
                row_x0 = row[0][0]
                row_x1 = row[0][2]
                row_y0 = row[0][1]
                row_y1 = row[0][-1]
                start = row[-1][-1] + 1
                for col in col_vals:
                    if row[-1] not in row_match:
                        col_x0 = col[0][0]
                        col_x1 = col[0][2]
                        min_x = min(col_x0, col_x1)
                        max_x = max(col_x0, col_x1)
                        for word_index, word_element in enumerate(final_words[start:]):
                            word_x0 = word_element['x0']
                            word_x1 = word_element['x1']
                            word_y0 = word_element['y0']
                            word_y1 = word_element['y1']
                            word_center = word_x0 + ((word_x1 - word_x0) / 2)
                            word_y_center = word_y0 + ((word_y1 - word_y0) / 2)
                            append_idx = start + word_index
                            if append_idx in matches:
                                continue
                            elif min_x < word_center < max_x and row_y0 < word_y_center < row_y1:
                                #                         print("x_center:",word_center,"y_center:",word_y_center)
                                #                         print("x_boundaries:",col_x0,col_x1)
                                #                         print("y_boundaries:",row_y0,row_y1)
                                matches.append(start + word_index)
                                # print(word_element)
                                row_match.append(row[-1])
                                break
                    else:
                        break
            # print(matches)
            # print(row_match)
            return matches

        def look_for_columns(word_list, word, words_with_location,value_split=True):
            # print()
            # print(word, 'I am column')
            w_c = word
            if value_split:
                word_split = [i for i in word.split(" ") if len(i) > 0]
            else:
                word_split = [word]
            indexes = []
            w_indexes = []
            for word_idx, word in enumerate(word_list):
                if word in word_split:
                    w_indexes.append(word_idx)
                    indexes.append(word_split.index(word))
            out = find_lexical_sequence_1(indexes, w_indexes, value_check=True)
            # print(out)
            col_points = dict()
            col_points[w_c] = []
            for col in out:
                pts = get_points([words_with_location[col[0]], words_with_location[col[-1]]])
                main_pts = (pts, (col[0], col[-1]))
                col_points[w_c].append(main_pts)
            # print('***** col points *****')
            # print(col_points)
            # print('-' * 10)
            return col_points

        def find_lexical_sequence_1(indexes, w_indexes, value_check=False):
            if len(indexes) == 0:
                return []
            check = len(set(indexes))
            out = []
            buffer = []
            s = indexes[0]
            for idx, i in enumerate(indexes[1:]):
                if s - i == -1:
                    buffer.append(w_indexes[idx + 1])
                else:
                    if len(buffer) > 0:
                        out.append([w_indexes[w_indexes.index(buffer[0]) - 1]] + buffer)
                    buffer = []
                s = i
            if len(buffer) > 0 and buffer not in out:
                out.append([w_indexes[w_indexes.index(buffer[0]) - 1]] + buffer)
            main_out = []
            for l in out:
                if len(l) == check:
                    if not value_check:
                        if max(l) - min(l) <= len(l) - 1:
                            main_out.append(l)
                    else:
                        if max(l) - min(l) <= 10:
                            main_out.append(l)

            if value_check and len(sorted(set(indexes))) == 1 and len(main_out) == 0:
                for i in w_indexes:
                    main_out.append([i, i])
            return main_out


        #word_list = final_words; words_with_location = words;
        def get_values(word_list, words_with_location):
            #table_header_found = ''
            table_header_found = []
            for table_header in table_headers:
                if type(table_header) == str:
                    word_split = [i for i in [table_header] if len(i) > 0]
                    indexes = []
                    w_indexes = []
                    for word_idx, word in enumerate(word_list):
                        if word.strip() in word_split:
                            w_indexes.append(word_idx)
                            indexes.append(word_split.index(word.strip()))
                    if len(indexes) > 0:
                        lex_out = find_lexical_sequence_1(indexes, w_indexes,value_check=True)
                        if len(lex_out) > 0 and len(set(lex_out[0])) == len(word_split):
                            # st.write("lex_out",lex_out)
                            # st.write("word_split",word_split)

                            #table_header_found = table_header
                            table_header_found.append(table_header)
                            print("=============$$$$$$$$$$==========")                    
                            print(table_header_found)
                            print("=============$$$$$$$$$$==========")
                            #break
            if len(table_header_found) > 0:
                # print(f'found this header {table_header_found} and lex is {lex_out}')
                print("=============$$$$$$$$$$==========")                    
                print(table_headers)
                print("=============$$$$$$$$$$==========")

                search_keys = reference[reference['PDF Table Header'].isin(table_header_found)]
                search_columns = reference[reference['PDF Table Header'].isin(table_header_found)]['columns_to_extract']
                search_keys = search_keys['PDF Key'].to_list()
                final_output = {}
                if len(search_columns) > 0:
                    search_columns = [i for i in search_columns.to_list() if type(i) == str]
                    need_to_search = []
                    for i in search_columns:
                        for j in i.split(','):
                            j = j.strip()
                            if j not in need_to_search:
                                need_to_search.append(j)
                    # print(need_to_search, '*******************')
                    for col in need_to_search:
                        c_pts = look_for_columns(word_list, col, words_with_location,value_split=False)
                        # print(search_keys)
                        a = {}
                        v = []
                        z = []
                        v_m = {}
                        for word in search_keys:
                            # print('*************')
                            # print(word)
                            # print(c_pts)
                            output = look_for_key_vals(word_list, word, words_with_location,value_split=False)
                            word_out = detect_values_nag(output, c_pts, word_list, words_with_location)
                            print("Search key: {0} :::: Value : {1}".format(word,word_out))
                            if word_out is not None and len(word_out) > 0:
                                # print(word)
                                # print(word_list[word_out[0]])
                                if word not in final_output:
                                    final_output[word] = {}
                                final_output[word][col] = word_list[word_out[0]]
                                # print('///////////////')
                            else:
                                # print('---------------')
                                print(" ")
                # print(':) final output')
                # print(final_output)
                if len(final_output) > 0:
                    subset = reference[['PDF Table Header Org', 'PDF Key Org', 'PDF Key', 'columns_to_extract_org',
                                        'columns_to_extract', 'Classification category']].copy()
                    subset['key_values'] = subset['PDF Key'].apply(lambda x: final_output.get(x))
                    # print(type(subset.key_values.iloc[0]))
                    buffer = []
                    for i in range(len(subset)):
                        key = subset['columns_to_extract'].iloc[i]
                        val_store = subset['key_values'].iloc[i]
                        if type(val_store) == dict:
                            buffer.append(val_store.get(key))
                        else:
                            buffer.append(None)
                    subset['needed_values'] = buffer
                    return subset



        # def tab_ext():
        #     s = []
        #     global rowss
        #     global he
        #     global endk
        #     global endc

        #     if ss is not None:
        #         rowss = ss.values.tolist()

        #         # fname = upload_file.rstrip("pdf").rstrip("PDF").rstrip(".")+str("_output")+str(".xlsx")
        #         # fname = str("Table--output")+str(".xlsx")


        #         for i in range(len(rowss)):
        #             he, endk,endc = rowss[i]

        #             s.append(dataframe(extract(he, endk,endc)))
        #             # dfs = s[i]
        #             s[i].to_excel('Table{}.xlsx'.format(i), index = False)

        #         frames = [ f for f in s ]
        #         result = pd.concat(frames,ignore_index=True)
        #         table = result.to_excel('FullTable.xlsx', index = False)

        #         # tablex = result.to_json(orient="split",force_ascii=False)



        #         # st.write(rowss)
        #         # multi()
        #         # su = st.success("Extraction completed successfully")

        #     # create_project_history()
        #         # Run_number = str(rn)
        #         # project_id = str(int(pp))
        #         # input_loc  = ref 
        #         # output_file_name = str("output:1->>  ")+str(patyh)+str("<<>>")+str("output:2->> ")+str(fname)
        #         print('_'*50)
        #         print("Table Extraction Started ")
        #         print('_'*50)
        #         print(frames)
        #         # Status1 = str("success")
        #         # cf1 = st.button("Confirm And Verify")
        #         # if Status1:
        #         #     print(pd.DataFrame({'Key_Name': ["username","Run_number: " ,"project_id:","Input_File_location:","output_file_names"],'Value_Name': [username,Run_number,project_id,input_loc,output_file_name],}))
        #         #     add_project_history(username,Run_number,project_id,input_loc,output_file_name,Status1)

        #     elif ss is None:
        #         print("No keys to extract table")    
        #     return result            
        # def tab_ext():
        #     global rowss

        #     if ss is not None:

        #         print("Table Extracton Started Please Wait...")
        #         rowss = ss.values.tolist()
        #         result1 = multi()
        #         print("Result1 value is:", result1)
        #         return result1
        #         # st.write(rowss)
        #         # result = pd.concat(multi(), ignore_index=True)
        #         # table = result.to_excel('FullTable.xlsx', index = False)

        #         # # su = st.success("Extraction completed successfully")
        #         # print(multi())

        #     # create_project_history()
        #         # Run_number = str(rn)
        #         # project_id = str(int(pp))
        #         # input_loc  = ref 
        #         # output_file_name = str("output:1->>  ")+str(patyh)+str("<<>>")+str("output:2->> ")+str(fname)
        #         # output_file_name = str("output:2->> ")+str(fname)
        #         # Status1 = str("success")
        #         # # cf1 = st.button("Confirm And Verify")
        #         # if Status1:
        #         #     st.write(pd.DataFrame({'Key_Name': ["username","Run_number: " ,"project_id:","Input_File_location:","output_file_names"],'Value_Name': [username,Run_number,project_id,input_loc,output_file_name],}))
        #         #     add_project_history(username,Run_number,project_id,input_loc,output_file_name,Status1)

        #     elif ss is None:
        #         print("No keys to extract table")
        #         result1 = pd.DataFrame({"Message":"No keys to extract table"},index=[0])
        #     return result1
        # def truncate_tb(trt):
        #     global zx
        #     heads1 = input("Enter Table Name to get Truncate Values")
        #     if heads1:
        #         lengths = input("Enter length of Table right side")
        #         if lengths:
        #             lengths = int(lengths)
        #             heads1 = heads1.lower()
        #             zx = [i for i in trt if heads1 in i]
        #             zx = zx[0]
        #             # st.write(zx)

        #             multi_tru(lengths) 
    #         try:    
        final_out = run_extraction(fpath,cpath)
        print(final_out)
    #         except Exception as err:
    #             exception_error = "run extraction code failed"
    #             final_out = pd.DataFrame({"error":err},index=[0])

        # print("---------------------------------------")


        # try:    
        #     data=tab_ext()
        # except Exception as err:
        #     exception_error = "table extraction code failed"
        #     data = pd.DataFrame({"error":err},index=[0])   
        ## final_out = run_extraction(fpath,cpath)
        # print("---------------------------------------")
        # data=tab_ext()
        #import requests
        #put_url = "https://devifusionbackend.azurewebsites.net/service/job/625115f34de2671666ab0468"
        #headers = {"content-type": "application/json", "Authorization": "Api-Key xUWYOMvrv27ppIvuMbo6Tn8mb2tEunMt"}
        #payload = {"ouputtable":final_out.to_dict(orient="records"),"fulltable":data.to_dict(orient="records")}
        if len(final_out) > 0:
            global payload2
            print('final_out column values are:\n', final_out[0].columns.tolist())
            payload2 = {"Key_Values":final_out[0].to_dict(orient="records"),"Specific_column":final_out[1].to_dict(orient="records"),"fulltable":final_out[-1].to_dict(orient="records")}
        else:
            payload2 = 'Payload2 is null'

        return payload2
    #         {"result":apistatus}
    except Exception as err:
        print('error is:', err)
        return f'Payload2 is null:{err}'

pdf_path = 'forms/Sample 2_Fed1065K1_TY2021.pdf'
# file_name = pdf_path.split('/')[1].split('.')[0]
ref_path = 'ref.csv'
result = main(pdf_path, ref_path)
# df_result = pd.DataFrame(result['Key_Values'])
# df_result.to_excel(f'{file_name}.xlsx', index=False)
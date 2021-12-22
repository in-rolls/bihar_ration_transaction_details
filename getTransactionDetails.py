# -*- coding: utf-8 -*-
"""
Parse transaction history

Four examples links

# scenario one - no table: rc not found
http://epos.bihar.gov.in/SRC_Trans_Details.jsp?src_no=10010070054003510078&month=11&year=2021

# scenario two - two tables: rc + entitlement found, transaction not found
http://epos.bihar.gov.in/SRC_Trans_Details.jsp?src_no=102070109622059900004377&month=11&year=2021

# scenario three - three tables: rc + entitlement + transaction found, authentication not found
http://epos.bihar.gov.in/SRC_Trans_Details.jsp?src_no=102050106021876900000505&month=11&year=2021

# scenario three - four tables: all rc + entitlement + transaction + authentication found
http://epos.bihar.gov.in/SRC_Trans_Details.jsp?src_no=102080110922129600003063&month=11&year=2021

"""

# prelim 
import pandas as pd
from bs4 import BeautifulSoup
import requests, os, re, time
from tqdm import tqdm

# definitions
work_dir = "C:/Users/aadit/Arthashala Dropbox/aepds/data_epos/SRC_Trans_Details"
filename = "C:/Users/aadit/Arthashala Dropbox/aepds/data_epos/SRC_Trans_Details/sample_303556url.txt"

# make list of links
my_file = open(filename, "r")
content = my_file.read()
url_list = content.split("\n")
my_file.close()

#%% 

# create an empty dictionary with 24 keys
# data = {'rc_id': [], 'ntables': [], 'rc_nf': [], 'trans_nf': [], 'header' : [], 'ent_wheat': [], 'ent_rice': [], 'ent_pmgkay_wheat': [], 'ent_pmgkay_rice': [], 'slno': [], 'member': [], 'avail_fps': [], 'month': [], 'year': [], 'avail_date': [], 'avail_type': [], 'avail_wheat': [], 'avail_rice': [], 'avail_pmgkay_wheat_old': [], 'avail_pmgkay_rice_old': [], 'avail_pmgkay_whole_chana': [], 'avail_pmgkay_pulse': [], 'avail_pmgkay_wheat': [], 'avail_pmgkay_rice': []}
data = {'rc_id': [], 'ntables': [], 'rc_nf': [], 'trans_nf': [], 'district' : [], 'block': [], 'fpsid': [], 'scheme': [], 'nunits': [], 'ent_wheat': [], 'ent_rice': [], 'ent_pmgkay_wheat': [], 'ent_pmgkay_rice': [], 'slno': [], 'member': [], 'avail_fps': [], 'month': [], 'year': [], 'avail_date': [], 'avail_type': [], 'avail_wheat': [], 'avail_rice': [], 'avail_pmgkay_wheat_old': [], 'avail_pmgkay_rice_old': [], 'avail_pmgkay_whole_chana': [], 'avail_pmgkay_pulse': [], 'avail_pmgkay_wheat': [], 'avail_pmgkay_rice': []}

#%% 

# use tqdm to track progress 
for i in tqdm(range(len(url_list[:1000]))):
    # initalize 
    url = url_list[i]

# loop over links 
# for url in url_list[730:740]: 
    # print("Query ", url)
    
    # turn request to a soup
    r = requests.get(url)
    # time.sleep(1)
    html = r.text
    soup = BeautifulSoup(html)
    
    # key 1: grab the rc id from the url
    rc = re.findall("\d+", url)[0]
    data['rc_id'].append(rc)

    # key 2: number of tables 
    ntables = len(soup.find_all('table'))
    data['ntables'] = ntables

    # scenario: rc cannot be found
    if ntables == 0:
        assert len(soup.body.find_all(text = re.compile("details not found", re.IGNORECASE))) == 1
        # there should be 22 more keys
        data['rc_nf'].append('1')
        data['trans_nf'].append('-9')
        # data['header'].append('-9')
        data['district'].append('-9')
        data['block'].append('-9')
        data['fpsid'].append('-9')
        data['scheme'].append('-9')        
        data['nunits'].append('-9')        
        data['ent_wheat'].append('-9')
        data['ent_rice'].append('-9')
        data['ent_pmgkay_wheat'].append('-9')
        data['ent_pmgkay_rice'].append('-9')
        data['slno'].append('-9')
        data['member'].append('-9')
        data['avail_fps'].append('-9')
        data['month'].append('-9')
        data['year'].append('-9')
        data['avail_date'].append('-9')
        data['avail_type'].append('-9')
        data['avail_wheat'].append('-9')
        data['avail_rice'].append('-9')
        data['avail_pmgkay_wheat_old'].append('-9')
        data['avail_pmgkay_rice_old'].append('-9')
        data['avail_pmgkay_whole_chana'].append('-9')
        data['avail_pmgkay_pulse'].append('-9')
        data['avail_pmgkay_wheat'].append('-9')
        data['avail_pmgkay_rice'].append('-9')
    # scenario: rc found but transaction not found
    elif ntables == 2:
        assert len(soup.body.find_all(text = re.compile("details not found", re.IGNORECASE))) == 1
        data['rc_nf'].append('0')
        data['trans_nf'].append('-8')
        # uidstatus is the first table 
        # and the second row in the first table has the basic info
        tab1 = soup.find_all('table')[0]
        tab1_row2 = tab1.find_all('tr')[1] # temp hack
        # data['header'].append(tab1_row2.text.strip()) # temp hack
        district = re.findall(r"District : (.*)", tab1_row2.get_text())
        data['district'].append(district[0].strip())
        block = re.findall(r"Taluk : (.*)", tab1_row2.get_text())
        data['block'].append(block[0].strip())
        fpsid = re.findall(r"FPS Id : (.*)", tab1_row2.get_text())
        data['fpsid'].append(fpsid[0].strip())
        scheme = re.findall(r"Scheme : (.*)", tab1_row2.get_text())
        data['scheme'].append(scheme[0].strip())
        nunits = re.findall(r"No of Units : (.*)", tab1_row2.get_text())
        data['nunits'].append(nunits[0].strip())
        # entitlement is the second table
        # and the third row has the entitlement info
        tab2 = soup.find_all('table')[1] 
        tab2_row3 = tab2.find_all('tr')[2]
        cols_tab2_row3 = tab2_row3.find_all('td')
        if len(cols_tab2_row3) == 4:
            # there should be 19 keys, because three are mentioned above (rc_nf, trans_nf, header)
            data['ent_wheat'].append(cols_tab2_row3[0].text.strip())
            data['ent_rice'].append(cols_tab2_row3[1].text.strip())
            data['ent_pmgkay_wheat'].append(cols_tab2_row3[2].text.strip())
            data['ent_pmgkay_rice'].append(cols_tab2_row3[3].text.strip())
            data['slno'].append('-8')
            data['member'].append('-8')
            data['avail_fps'].append('-8')
            data['month'].append('-8')
            data['year'].append('-8')
            data['avail_date'].append('-8')
            data['avail_type'].append('-8')
            data['avail_wheat'].append('-8')
            data['avail_rice'].append('-8')
            data['avail_pmgkay_wheat_old'].append('-8')
            data['avail_pmgkay_rice_old'].append('-8')
            data['avail_pmgkay_whole_chana'].append('-8')
            data['avail_pmgkay_pulse'].append('-8')
            data['avail_pmgkay_wheat'].append('-8')
            data['avail_pmgkay_rice'].append('-8')
    elif ntables == 3 or ntables == 4:
        # rc and tran found
        data['rc_nf'].append('0')
        data['trans_nf'].append('0')
        # uidstatus is the first table 
        # and the second row in the first table has the basic info
        tab1 = soup.find_all('table')[0]
        tab1_row2 = tab1.find_all('tr')[1] # temp hack
        # data['header'].append(tab1_row2.text.strip()) # temp hack
        district = re.findall(r"District : (.*)", tab1_row2.get_text())
        data['district'].append(district[0].strip())
        block = re.findall(r"Taluk : (.*)", tab1_row2.get_text())
        data['block'].append(block[0].strip())
        fpsid = re.findall(r"FPS Id : (.*)", tab1_row2.get_text())
        data['fpsid'].append(fpsid[0].strip())
        scheme = re.findall(r"Scheme : (.*)", tab1_row2.get_text())
        data['scheme'].append(scheme[0].strip())
        nunits = re.findall(r"No of Units : (.*)", tab1_row2.get_text())
        data['nunits'].append(nunits[0].strip())
        # entitlement is the second table
        # and the third row has the entitlement info
        tab2 = soup.find_all('table')[1] 
        tab2_row3 = tab2.find_all('tr')[2] # hack
        cols_tab2_row3 = tab2_row3.find_all('td')    
        if len(cols_tab2_row3) == 4:
            # there should be 4 keys
            data['ent_wheat'].append(cols_tab2_row3[0].text.strip())
            data['ent_rice'].append(cols_tab2_row3[1].text.strip())
            data['ent_pmgkay_wheat'].append(cols_tab2_row3[2].text.strip())
            data['ent_pmgkay_rice'].append(cols_tab2_row3[3].text.strip())
        # authentication table may or may not be there
        # so, transaction history table is the last table 
        tab_last = soup.find_all('table')[-1]
        rows = tab_last.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) == 15:
                # there should be 15 keys
                data['slno'].append(cols[0].text.strip())
                data['member'].append(cols[1].text.strip())
                data['avail_fps'].append(cols[2].text.strip())
                data['month'].append(cols[3].text.strip())
                data['year'].append(cols[4].text.strip())
                data['avail_date'].append(cols[5].text.strip())
                data['avail_type'].append(cols[6].text.strip())
                data['avail_wheat'].append(cols[7].text.strip())
                data['avail_rice'].append(cols[8].text.strip())
                data['avail_pmgkay_wheat_old'].append(cols[9].text.strip())
                data['avail_pmgkay_rice_old'].append(cols[10].text.strip())
                data['avail_pmgkay_whole_chana'].append(cols[11].text.strip())
                data['avail_pmgkay_pulse'].append(cols[12].text.strip())
                data['avail_pmgkay_wheat'].append(cols[13].text.strip())
                data['avail_pmgkay_rice'].append(cols[14].text.strip())
        
#%%

# convert to dataframe
print("Convert to df")
datadf = pd.DataFrame(data)

print("Export to csv")
outfile = os.path.join(work_dir, "SRC_Trans_Details.csv")
datadf.to_csv(outfile)
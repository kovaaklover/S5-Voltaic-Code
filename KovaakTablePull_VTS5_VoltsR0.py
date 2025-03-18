import csv
import statistics
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os

# KOVAAKs LEADERBOARD IDs
Leaderboard_ID = [
    98059,98285,103210,98030,106186,98054,98060,101560,101370,101744,98047,103765,104868,104835,104862,104830,105132,103262,
    98330,98333,103209,98029,105828,98055,98331,98045,101369,101743,98048,103353,104865,105176,104863,105142,104890,103261,
    98023,98024,98025,98026,105776,98012,98255,98297,100140,100768,98253,103114,104961,104864,104866,105143,104833,98619,
]


# S5 RANK REQUIREMENTS
RankReq = [
[0,555,660,745,800,770,850,930,980,910,1020,1130,1250],
[0,390,500,600,720,600,690,780,860,680,810,920,1030],
[0,820,915,1010,1110,1120,1220,1300,1380,1310,1410,1510,1620],
[0,990,1090,1190,1290,1310,1400,1490,1560,1510,1620,1740,1880],
[0,620,740,850,980,940,1040,1140,1230,1090,1200,1350,1480],
[0,375,460,540,640,610,690,770,860,740,840,950,1060],
[0,1425,1900,2375,2950,2150,2550,2925,3200,2700,3100,3475,3825],
[0,2400,2750,3050,3375,2775,3175,3500,3750,2975,3300,3600,3875],
[0,1525,1900,2250,2650,2175,2550,2900,3175,2750,3175,3525,3825],
[0,2100,2500,2825,3100,2550,2850,3100,3350,2875,3200,3500,3725],
[0,2125,2550,2975,3450,2775,3200,3550,3875,3150,3550,3875,4250],
[0,1575,1950,2400,2900,2750,3175,3525,3825,3100,3475,3800,4125],
[0,845,940,1030,1090,1110,1180,1230,1280,1280,1360,1440,1520],
[0,640,730,810,890,880,950,1020,1080,1010,1120,1190,1270],
[0,315,355,390,430,390,430,460,490,430,470,510,540],
[0,390,450,510,560,500,560,610,650,520,580,640,700],
[0,340,380,420,450,420,460,485,520,450,490,530,560],
[0,290,340,390,445,450,490,540,580,530,580,630,670]
]

NLimits = [855,840,1210,1390,1110,740,3525,3700,3050,3375,3925,3400,1150,970,470,610,480,500]

ILimits = [1030,940,1460,1630,1320,950,3475,4000,3450,3600,4200,4125,1330,1140,520,690,555,620]

# S5 RANKS
Ranks = ["N/A", "Unranked", "Iron", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Jade", "Master", "Grandmaster", "Nova", "Astra", "Celestial"]

# FUNCTION TO PROCESS EACH PAGE OF EACH LEADERBOARD (FUNCTION CALLED VIA THREADING)
def process_leaderboard(leaderboard_id, page, session, itera, Count, score_lock, Score_Dic, RankReq):
    result = []

    # API DATA PULL
    try:
        r = session.get(f"https://kovaaks.com/webapp-backend/leaderboard/scores/global?leaderboardId={leaderboard_id}&page={page}&max=100").json()
        print(f"Leaderboard {leaderboard_id}. Page: {page} data pull.")

        # ITERATE THROUGH ALL DATA ROWS (100 LEADERBOARD ENTRIES) IN THE API PULL
        for Data in r['data']:
            try:
                Steam_Name = Data['steamAccountName']
                Steam_ID = Data['steamId']
                Score = Data['score']

                VoltsN = 0
                VoltsI = 0
                VoltsA = 0


                # LOCK
                with score_lock:

                    # IF STEAM ID WAS NOT YET SEEN CREATE KEY AND SET VOLTS TO ZERO
                    if Steam_ID not in Score_Dic:
                        Score_Dic[Steam_ID] = [0] * (163)
                        Score_Dic[Steam_ID][111] = Steam_Name

                    # FOR NOVICE LEADERBOARDS
                    if itera == 1:

                        # ITERATE THROUGH RANKS
                        for iii in range(1, 5):
                            if iii == 4 and RankReq[Count][iii] <= Score:

                                VoltsN = iii * 100 + (Score - RankReq[Count][iii]) * 100 / (NLimits[Count] - RankReq[Count][iii])
                                if VoltsN > 500:
                                    VoltsN = 500
                                Score_Dic[Steam_ID][54 + Count+0] = VoltsN

                            elif RankReq[Count][iii] <= Score:
                                Score_Dic[Steam_ID][Count] = Score
                                VoltsN = (iii) * 100 + (Score - RankReq[Count][iii]) * 100 / (RankReq[Count][iii+1] - RankReq[Count][iii])
                                Score_Dic[Steam_ID][54 + Count+0] = VoltsN

                            elif iii==1:
                                Score_Dic[Steam_ID][Count] = Score
                                VoltsN = 0 + (Score - 0) * 100 / (RankReq[Count][iii] - 0)
                                Score_Dic[Steam_ID][54 + Count+0] = VoltsN

                        Score_Dic[Steam_ID][108] += VoltsN/18


                    # FOR INTERMEDIATE LEADERBOARD
                    elif itera == 2:

                        # ITERATE THROUGH RANKS
                        for iii in range(5, 9):
                            if iii == 8 and RankReq[Count][iii] <= Score:
                                VoltsI = iii * 100 + (Score - RankReq[Count][iii]) * 100 / (ILimits[Count] - RankReq[Count][iii])
                                if VoltsI > 900:
                                    VoltsI = 900
                                Score_Dic[Steam_ID][54 + Count+18] = VoltsI

                            elif RankReq[Count][iii] <= Score:
                                Score_Dic[Steam_ID][Count+18] = Score
                                VoltsI = iii * 100 + (Score - RankReq[Count][iii]) * 100 / (RankReq[Count][iii+1] - RankReq[Count][iii])
                                Score_Dic[Steam_ID][54 + Count+18] = VoltsI

                            elif iii == 5:
                                Score_Dic[Steam_ID][Count+18] = Score
                                VoltsI = 0 + (Score - 0) * 500 / (RankReq[Count][iii] - 0)
                                Score_Dic[Steam_ID][54 + Count+18] = VoltsI

                        Score_Dic[Steam_ID][109] += VoltsI/18

                    # FOR ADVANCED LEADERBOARD
                    elif itera == 3:

                        # ITERATE THROUGH RANKS
                        for iii in range(9, 13):
                            if iii == 12 and RankReq[Count][iii] <= Score:
                                VoltsA = 1200
                                Score_Dic[Steam_ID][54 + Count+36] = VoltsA

                            elif RankReq[Count][iii] <= Score:
                                Score_Dic[Steam_ID][Count+36] = Score
                                VoltsA = iii * 100 + (Score - RankReq[Count][iii]) * 100 / (RankReq[Count][iii+1] - RankReq[Count][iii])
                                Score_Dic[Steam_ID][54 + Count+36] = VoltsA

                            elif iii == 9:
                                Score_Dic[Steam_ID][Count+36] = Score
                                VoltsA = 0 + (Score - 0) * 900 / (RankReq[Count][iii] - 0)
                                Score_Dic[Steam_ID][54 + Count+36] = VoltsA

                        Score_Dic[Steam_ID][110] += VoltsA/18

            except KeyError:
                continue
    except Exception as e:
        print(f"Error processing leaderboard {leaderboard_id} page {page}: {e}")
    return result


# Main code with threading and lock protection
Score_Dic = {}
score_lock = Lock()  # Create a lock for protecting shared resources

# START THREADER
with ThreadPoolExecutor(max_workers=10) as executor:
    Count = 0
    itera = 1
    futures = []
    session = requests.Session()

    # ITERATE THROUGH ALL LEADERBOARDS
    for i in range(len(Leaderboard_ID)):

        r = session.get(f"https://kovaaks.com/webapp-backend/leaderboard/scores/global?leaderboardId={Leaderboard_ID[i]}&page=0&max=100").json()
        Max_Page = r.get('total', 0) // 100
     #   Max_Page=10

        # ITERATE THROUGH ALL LEADERBOARD PAGES AND SEND TO FUNCTION
        for ii in range(Max_Page + 1):
            futures.append(executor.submit(process_leaderboard, Leaderboard_ID[i], ii, session, itera, Count, score_lock, Score_Dic, RankReq))

        # LOCK CRITERIA (NEEDED)
        with score_lock:
            Count += 1
            if Count >= 18 and itera == 1:
                Count = 0
                itera = 2
            elif Count >= 18 and itera == 2:
                Count = 0
                itera = 3

    # PROCESS RESULTS
    for future in as_completed(futures):
        future.result()  # No need to handle this since the processing is done within the function

    session.close()

# ITERATE THROUGH ALL KEYS IN DICTIONARY
Count = 0
for key, values in Score_Dic.items():
    RankN = values[54:72]
    RankI = values[72:90]
    RankA = values[90:108]

    # CALCULATE RANK VOLTS
    RN = statistics.harmonic_mean([int(max(RankN[0:2])), int(max(RankN[2:4])), int(max(RankN[4:6])), int(max(RankN[6:8])), int(max(RankN[8:10])), int(max(RankN[10:12])), int(max(RankN[12:14])), int(max(RankN[14:16])), int(max(RankN[16:18]))])
    values[112] = int(RN)

    RI = statistics.harmonic_mean([int(max(RankI[0:2])), int(max(RankI[2:4])), int(max(RankI[4:6])), int(max(RankI[6:8])), int(max(RankI[8:10])), int(max(RankI[10:12])), int(max(RankI[12:14])), int(max(RankI[14:16])), int(max(RankI[16:18]))])
    values[113] = int(RI)

    RA = statistics.harmonic_mean([int(max(RankA[0:2])), int(max(RankA[2:4])),int(max(RankA[4:6])), int(max(RankA[6:8])), int(max(RankA[8:10])), int(max(RankA[10:12])), int(max(RankA[12:14])), int(max(RankA[14:16])), int(max(RankA[16:18]))])
    values[114] = int(RA)

    # CALCULATE RANK FROM RANK VOLTS NOVICE
    for i in range(0, 5):

        # GET TASK RANKS
        for ii in range(0, 18):
            if values[ii+54] >= i * 100:
                values[122 + ii] = Ranks[i+1]
                values[140 + ii] = values[ii]

        # GET RANK NOVICE
        if values[112] >= i*100:
            values[115] = Ranks[i+1]

            if min([min(RankN[0:2]), min(RankN[2:4]), min(RankN[6:8]), min(RankN[8:10]), min(RankN[12:14]), min(RankN[14:16]), min(RankN[16:18])]) >= i*100 and i > 0:
                values[115] = values[115] + " Complete"
            values[120] = values[115]

    # CALCULATE RANK FROM RANK VOLTS INTERMEDIATE
    for i in range(5, 9):

        # GET TASK RANKS
        for ii in range(0, 18):
            if values[ii+18+54] >= i * 100:
                values[122 + ii] = Ranks[i+1]
                values[140 + ii] = values[ii + 18]

        # GET RANK INTERMEDIATE
        if values[113] >= i*100:
            values[116] = Ranks[i+1]

            if min(RankI) >= i*100:
                values[116] = values[116] + " Complete"
            values[120] = values[116]

    # CALCULATE RANK FROM RANK VOLTS ADVANCED
    for i in range(9, 14):

        # GET TASK RANKS
        for ii in range(0, 18):
            if values[ii+36+54] >= i * 100:
                values[122 + ii] = Ranks[i+1]
                values[140 + ii] = values[ii + 36]

        # GET RANK ADVANCED
        if values[114] >= i*100:
            values[117] = Ranks[i+1]

            if min(RankA) >= i*100:
                values[117] = values[117] + " Complete"
            values[120] = values[117]

    # RANK WITHOUT COMPLETE
    if values[120].endswith(" Complete"):
        values[121] = values[120][:-9]  # Remove exactly 9 characters (the length of " Complete")
    else:
        values[121] = values[120]

    # MAKE IT SO THAT MASTER PLAYERS ARE ALWAYS WORSE THAN GM PLAYERS
    if values[114] < 900:
        values[110] = 0

    if values[113] < 500:
        values[109] = 0

    # COUNT OF RELEVANT ENTRIES
    if values[112] > 0 or values[113] > 0 or values[114] > 0:
        Count += 1

# SORT NOVICE COMPLETE POINTS THEN INTERMEDIATE COMPLETE POINTS THEN ADVANCED COMPLETE POINTS
Score_Dic_S = dict(sorted(Score_Dic.items(), key=lambda item: (item[1][110], item[1][109], item[1][108]), reverse=True))
Per = 0
for key, values in Score_Dic_S.items():
    if values[112] > 0 or values[113] > 0 or values[114] > 0:
        values[118] = Per+1
        values[119] = round(1 - Per / Count, 6)
        Per += 1

        values[158] = values[112]
        values[159] = values[113]
        values[160] = values[114]

        # IF LESS THAN MASTER AND GRANDMASTER SET ADVANCED ENERGY TO ZERO
        if values[113] < 800 and values[114] < 900:
            values[160] = 0

        # IF LESS THAN GOLD AND PLAT SET MASTER ENERGY TO ZERO
        if values[112] < 400 and values[113] < 500:
            values[159] = 0

# SORT NOVICE VOLTS THEN INTERMEDIATE VOLTS THEN ADVANCED ENERGY
Score_Dic_S = dict(sorted(Score_Dic.items(), key=lambda item: (item[1][160], item[1][159], item[1][158]), reverse=True))
Per = 0
for key, values in Score_Dic_S.items():
    if values[112] > 0 or values[113] > 0 or values[114] > 0:
        values[161] = Per+1
        values[162] = round(1 - Per / Count, 6)
        Per += 1

header = ['PlayerID',

'VT Pasu Novice S5','VT Popcorn Novice S5','VT 1w4ts Novice S5','VT ww5t Novice S5','VT Frogtagon Novice S5','VT Floating Heads Novice S5',
'VT PGT Novice S5','VT Snake Track Novice S5','VT Aether Novice S5','VT Ground Novice S5','VT Raw Control Novice S5','VT Controlsphere Novice S5',
'VT DotTS Novice S5','VT EddieTS Novice S5','VT DriftTS Novice S5','VT FlyTS Novice S5','VT ControlTS Novice S5','VT Penta Bounce Novice S5',

'VT Pasu Intermediate S5','VT Popcorn Intermediate S5','VT 1w3ts Intermediate S5','VT ww5t Intermediate S5','VT Frogtagon Intermediate S5','VT Floating Heads Intermediate S5',
'VT PGT Intermediate S5','VT Snake Track Intermediate S5','VT Aether Intermediate S5','VT Ground Intermediate S5','VT Raw Control Intermediate S5','VT Controlsphere Intermediate S5',
'VT DotTS Intermediate S5','VT EddieTS Intermediate S5','VT DriftTS Intermediate S5','VT FlyTS Intermediate S5','VT ControlTS Intermediate S5','VT Penta Bounce Intermediate S5',

'VT Pasu Advanced S5','VT Popcorn Advanced S5','VT 1w2ts Advanced S5','VT ww5t Advanced S5','VT Frogtagon Advanced S5','VT Floating Heads Advanced S5',
'VT PGT Advanced S5','VT Snake Track Advanced S5','VT Aether Advanced S5','VT Ground Advanced S5','VT Raw Control Advanced S5','VT Controlsphere Advanced S5',
'VT DotTS Advanced S5','VT EddieTS Advanced S5','VT DriftTS Advanced S5','VT FlyTS Advanced S5','VT ControlTS Advanced S5','VT Penta Bounce Advanced S5',

'VT Pasu Novice S5 V','VT Popcorn Novice S5 V','VT 1w4ts Novice S5 V','VT ww5t Novice S5 V','VT Frogtagon Novice S5 V','VT Floating Heads Novice S5 V',
'VT PGT Novice S5 V','VT Snake Track Novice S5 V','VT Aether Novice S5 V','VT Ground Novice S5 V','VT Raw Control Novice S5 V','VT Controlsphere Novice S5 V',
'VT DotTS Novice S5 V','VT EddieTS Novice S5 V','VT DriftTS Novice S5 V','VT FlyTS Novice S5 V','VT ControlTS Novice S5 V','VT Penta Bounce Novice S5 V',

'VT Pasu Intermediate S5','VT Popcorn Intermediate S5 V','VT 1w3ts Intermediate S5 V','VT ww5t Intermediate S5 V','VT Frogtagon Intermediate S5 V','VT Floating Heads Intermediate S5 V',
'VT PGT Intermediate S5','VT Snake Track Intermediate S5 V','VT Aether Intermediate S5 V','VT Ground Intermediate S5 V','VT Raw Control Intermediate S5 V','VT Controlsphere Intermediate S5 V',
'VT DotTS Intermediate S5','VT EddieTS Intermediate S5 V','VT DriftTS Intermediate S5 V','VT FlyTS Intermediate S5 V','VT ControlTS Intermediate S5 V','VT Penta Bounce Intermediate S5 V',

'VT Pasu Advanced S5 V','VT Popcorn Advanced S5 V','VT 1w2ts Advanced S5 V','VT ww5t Advanced S5 V','VT Frogtagon Advanced S5 V','VT Floating Heads Advanced S5 V',
'VT PGT Advanced S5 V','VT Snake Track Advanced S5 V','VT Aether Advanced S5 V','VT Ground Advanced S5 V','VT Raw Control Advanced S5 V','VT Controlsphere Advanced S5 V',
'VT DotTS Advanced S5 V','VT EddieTS Advanced S5 V','VT DriftTS Advanced S5 V','VT FlyTS Advanced S5 V','VT ControlTS Advanced S5 V','VT Penta Bounce Advanced S5 V',


'Novice Complete Points', 'Intermediate Complete Points', 'Advanced Complete Points', 'Steam Name', 'Novice Energy',  'Intermediate Energy',  'Advanced Energy', 'Novice Rank',  'Intermediate Rank',  'Advanced Rank', 'Rank','Percentage','Max Rank','Base Rank',

'VT Pasu R','VT Popcorn R','VT 1w2ts R','VT ww5t R','VT Frogtagon R','VT Floating Heads R',
'VT PGT R','VT Snake Track R','VT Aether R','VT Ground R','VT Raw Control R','VT Controlsphere R',
'VT DotTS R','VT EddieTS R','VT DriftTS R','VT FlyTS R','VT ControlTS R','VT Penta Bounce R',

'VT Pasu S','VT Popcorn S','VT 1w2ts S','VT ww5t S','VT Frogtagon S','VT Floating Heads S',
'VT PGT S','VT Snake Track S','VT Aether S','VT Ground S','VT Raw Control S','VT Controlsphere S',
'VT DotTS S','VT EddieTS S','VT DriftTS S','VT FlyTS S','VT ControlTS S','VT Penta Bounce S',

'ADJ N E', 'ADJ I E', 'ADJ A E', 'E Rank', 'E Percentage',
]

header1 = [header[0]] + header[109:]

# CSV PRINT
#csv_file = 'output.csv'
#with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
#    writer = csv.writer(file)
#    writer.writerow(header)
#    for key, values in Score_Dic_S.items():
#        if values[112] > 0 or values[113] > 0 or values[114] > 0:
#            if values[111] is not None:
#                values[111] = values[111].encode('ascii', 'ignore').decode('ascii')
#            else:
#                values[111] = ''

#            writer.writerow([key] + values)

# GOOGLE SHEETS API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# JSON CREDENTIAL FILE PATH
creds_dict = json.loads(os.getenv('GSPREAD_CREDENTIALS'))
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

# AUTHORIZE THE CLIENT
client = gspread.authorize(creds)

# OPEN GOOGLE SHEET
sheet = client.open('S5_Voltaic').sheet1
sheet1 = client.open('S5_Voltaic_1').sheet1
sheet2 = client.open('S5_Voltaic_2').sheet1
sheet3 = client.open('S5_Voltaic_3').sheet1
sheet4 = client.open('S5_Voltaic_4').sheet1
sheet5 = client.open('S5_Voltaic_5').sheet1
sheet6 = client.open('S5_Voltaic_6').sheet1

sheet6.clear()
data = sheet5.get_all_values()
sheet6.update('A1', data)

sheet5.clear()
data = sheet4.get_all_values()
sheet5.update('A1', data)

sheet4.clear()
data = sheet3.get_all_values()
sheet4.update('A1', data)

sheet3.clear()
data = sheet2.get_all_values()
sheet3.update('A1', data)

sheet2.clear()
data = sheet1.get_all_values()
sheet2.update('A1', data)

sheet1.clear()
data = sheet.get_all_values()
sheet1.update('A1', data)

# CLEAR EXISTING DATA IN GOOGLE SHEET
sheet.clear()

# WRITE HEADERS TO FIRST ROW
sheet.append_row(header1)

# SEND DATA FROM DICTIONARY TO ARRAY
rows_to_update = []
for key, values in Score_Dic_S.items():
    if values[112] > 0 or values[113] > 0 or values[114] > 0:
        if values[111] is not None:
            values[111] = values[111].encode('ascii', 'ignore').decode('ascii')
        else:
            values[111] = ''

        values=values[108:]

        # Add the row to the list
        rows_to_update.append([key] + values)

# UPDATE GOOGLE SHEET WITH ALL ARRAY DATA
start_cell = 'A2'
sheet.update(rows_to_update, start_cell)

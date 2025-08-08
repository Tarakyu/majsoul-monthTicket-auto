import asyncio
import logging
import random
import uuid
import json
import aiohttp
import os
import gspread
import pytz

from datetime import datetime
from dotenv import load_dotenv
from ms.base import MSRPCChannel
from ms.rpc import Lobby
import ms.protocol_pb2 as pb
from google.protobuf.json_format import MessageToJson
from google.protobuf.json_format import MessageToDict
from oauth2client.service_account import ServiceAccountCredentials

from han_constants import HAN

load_dotenv()
uid = os.getenv("UID", "default_uid")
token = os.getenv("TOKEN", "default_token")
TOURNAMENT_ID = int(os.getenv("TOURNAMENT_ID", 0))

deviceId = f"web|{uid}"

MS_HOST = "https://mahjongsoul.game.yo-star.com/"
PASSPORT_HOST = "https://passport.mahjongsoul.com/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")


async def main():


    lobby, channel, version_to_force, accessTokenFromPassport = await connect()
    await login(lobby, version_to_force, accessTokenFromPassport)

    await channel.close()


async def connect():
    async with aiohttp.ClientSession() as session:
        async with session.get("{}version.json".format(MS_HOST)) as res:
            version = await res.json()
            logging.info(f"Version: {version}")
            version = version["version"]
            version_to_force = version.replace(".w", "")

        async with session.get("{}v{}/config.json".format(MS_HOST, version)) as res:
            config = await res.json()
            logging.info(f"Config: {config}")

            url = config["ip"][0]["region_urls"][0]["url"]
            passport_url = config["yo_service_url"][0]
            print(passport_url)

        async with session.get(url + "?service=ws-gateway&protocol=ws&ssl=true") as res:
            servers = await res.json()
            # mjjpgs.mahjongsoul.com:9663
            logging.info(f"Available servers: {servers}")

            servers = servers["servers"]
            server = random.choice(servers)
            endpoint = "wss://{}/gateway".format(server)

        async with session.post(
            passport_url + "/user/login/",
            data={
                "uid": uid,
                "token": token,
                "deviceId": deviceId,
            },
        ) as res:
            passport = await res.json()
            accessTokenFromPassport = passport["accessToken"]

    logging.info(f"Chosen endpoint: {endpoint}")
    channel = MSRPCChannel(endpoint)

    lobby = Lobby(channel)

    await channel.connect(MS_HOST)
    logging.info("Connection was established")

    return lobby, channel, version_to_force, accessTokenFromPassport


async def login(lobby, version_to_force, accessTokenFromPassport):
    logging.info("Login with username and password")

    heartBeat = pb.ReqHeatBeat()
    heartBeat.no_operation_counter = 1
    hbRes = await lobby.heatbeat(heartBeat)  # heartbeat는 로그인하기 전에 임의적으로 몇번 통신함

    reqFromSoulLess = pb.ReqOauth2Auth()
    reqFromSoulLess.type = 7
    reqFromSoulLess.code = accessTokenFromPassport
    reqFromSoulLess.uid = uid
    reqFromSoulLess.client_version_string = f"web-{version_to_force}"

    res = await lobby.oauth2_auth(reqFromSoulLess)

    token = res.access_token
    if not token:
        logging.error("Login Error:")
        logging.error(res)
        return False

    # reqOauth2Check = pb.ReqOauth2Check()
    # reqOauth2Check.type = 7
    # reqOauth2Check.access_token = token
    # resOauth2Check = await lobby.oauth2_check(reqOauth2Check)
    # print(resOauth2Check)
    # if not resOauth2Check.has_account:
    #     print("Invalid access token")
    #     return False

    reqOauth2Login = pb.ReqOauth2Login()
    reqOauth2Login.type = 7
    reqOauth2Login.access_token = token

    reqOauth2Login.reconnect = False
    reqOauth2Login.device.is_browser = True
    uuid_key = str(uuid.uuid1())
    reqOauth2Login.random_key = uuid_key
    reqOauth2Login.client_version_string = f"web-{version_to_force}"
    reqOauth2Login.gen_access_token = False
    reqOauth2Login.currency_platforms.append(2)

    resOauth2Login = await lobby.oauth2_login(reqOauth2Login)

    # await getMonthlyTicket(lobby)

    req = pb.ReqFetchCustomizedContestGameRecords(unique_id=TOURNAMENT_ID)
    res = await lobby.fetch_customized_contest_game_records(req)
    res_dict = MessageToDict(res)

    records = res_dict.get("recordList", [])

    data_sheet = connect_to_data_sheet()
    existing_uuids = get_existing_uuids(data_sheet)
    statistics_sheet = connect_to_statistics_sheet()
    hules_sheet = connect_to_hules_sheet()

    new_rows = []
    for record in records:
        if record["uuid"] not in existing_uuids:
            new_rows.append((record["startTime"], record))

    new_rows.sort(key=lambda x: int(x[0]))

    rows_to_append = []
    statistics_rows = []
    hule_rows = []
    for r in new_rows:
        parsed_row, seat_map = parse_game_record(r[1])
        rows_to_append.append(parsed_row)
        statistics, hules = await get_game_statistics(lobby, r[1]["uuid"], version_to_force)
        
        for seat in range(4):
            seat_stats = statistics["players"].get(seat, {})
            row = [
                r[1]["uuid"],
                seat_map[seat],
                statistics["total_kyoku"],
                seat_stats.get("riichi", 0),
                seat_stats.get("hora", 0),
                seat_stats.get("tsumo", 0),
                seat_stats.get("ron", 0),
                seat_stats.get("houju", 0),
                seat_stats.get("furo", 0),
                seat_stats.get("dama", 0),
                seat_stats.get("chase_riichi", 0),
            ]
            statistics_rows.append(row)
        for hule in hules:
            row = [r[1]["uuid"], seat_map[hule[0]], hule[1], hule[2]]
            hule_rows.append(row)


    if rows_to_append:
        data_sheet.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        statistics_sheet.append_rows(statistics_rows, value_input_option="USER_ENTERED")
        hules_sheet.append_rows(hule_rows, value_input_option="USER_ENTERED")

    print(f"총 {len(new_rows)}개의 새로운 게임 기록이 추가되었습니다.")

    return True

def connect_to_data_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open("카일색 대회전 기록지").worksheet("데이터")
    return sheet

def connect_to_statistics_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open("카일색 대회전 기록지").worksheet("국 통계")
    return sheet

def connect_to_hules_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open("카일색 대회전 기록지").worksheet("화료역")
    return sheet

def format_time(ts):
    KST = pytz.timezone('Asia/Seoul')
    return datetime.fromtimestamp(ts, tz=pytz.utc).astimezone(KST).strftime("%Y-%m-%d %H:%M")

def get_existing_uuids(sheet):
    uuid_col = sheet.col_values(20)  # uuid는 20번째 열 (패보 링크)
    return set(uuid_col[1:])  # 첫 줄은 헤더이므로 제외

def parse_game_record(record: dict) -> list:

    uuid = record["uuid"]
    start_time = format_time(int(record["startTime"]))
    end_time = format_time(int(record["endTime"]))
    deleted = "no"

    accounts = record.get("accounts", [])
    results = record.get("result", {}).get("players", [])

    # seat 있는 계정 먼저 매핑
    players_by_seat = {p["seat"]: p for p in accounts if "seat" in p}
    # seat 없는 계정 따로 저장
    players_without_seat = [p for p in accounts if "seat" not in p]

    row = [start_time, end_time, deleted]
    seat_map = {}

    for p in results:
        seat = p.get("seat")
        total = p.get("totalPoint", 0)
        part = p.get("partPoint1", 0)

        if seat is not None and seat in players_by_seat:
            info = players_by_seat[seat]
            seat_map[seat] = info.get("accountId", "")
        else:
            info = players_without_seat.pop(0) if players_without_seat else {}
            seat_map[0] = info.get("accountId", "")

        row.extend([
            info.get("accountId", ""),
            info.get("nickname", ""),
            part,
            round(total / 1000, 1)
        ])

        

    row.append(uuid)

    
    return row, seat_map

async def fetchGameRecordList(lobby):
    reqGameRecordList = pb.ReqGameRecordList()
    resGameRecordList = await lobby.fetch_game_record_list(reqGameRecordList)

    print(resGameRecordList)
    dict_data = MessageToDict(resGameRecordList)
    json_string = json.dumps(dict_data, ensure_ascii=False, indent=2)
    with open("result.txt", "w", encoding="utf-8") as f:
        f.write(json_string)

async def get_game_statistics(lobby, uuid, version_to_force):
    req = pb.ReqGameRecord()
    req.game_uuid = uuid
    req.client_version_string = f"web-{version_to_force}"
    res = await lobby.fetch_game_record(req)

    record_wrapper = pb.Wrapper()
    record_wrapper.ParseFromString(res.data)
    with open("./results/record_wrapper.txt", "w", encoding="utf-8") as f:
        f.write(MessageToJson(record_wrapper))

    game_details = pb.GameDetailRecords()
    game_details.ParseFromString(record_wrapper.data)
    with open("./results/game_details2.txt", "w", encoding="utf-8") as f:
        f.write(MessageToJson(game_details))

    result = analyze_game_log(MessageToDict(game_details))

    ## 화료역 추가하는 코드
    records = [action.result for action in game_details.actions if action.type == 1]

    game_records_count = len(records)
    logging.info("Found {} game records".format(game_records_count))

    hules = []
    round_record_wrapper = pb.Wrapper()

    for i in range(0, game_records_count):
        round_record_wrapper.ParseFromString(records[i])

        if round_record_wrapper.name == ".lq.RecordHule":
            record_hule = pb.RecordHule()
            record_hule.ParseFromString(round_record_wrapper.data)
            hule = MessageToDict(record_hule)["hules"][0]
            for fan in hule["fans"]:
                if "val" in fan:
                    fan_id = fan["id"]
                    fan_name = HAN.get(fan_id, f"알 수 없는 역({fan_id})")
                    hules.append([hule.get("seat", 0), fan_name, fan.get("val", 0)])

    return result, hules

def analyze_game_log(log_json: dict) -> dict:
    actions = log_json.get("actions", [])

    current_kyoku = 0
    stats = {
        seat: {
            "ron": set(),
            "tsumo": set(),
            "houju": set(),
            "riichi": set(),
            "furo": set(),
            "dama": set(),
            "chase_riichi": set()
        } for seat in range(4)
    }

    riichi_declared_in_kyoku = set()  # 현재 국에서 누가 리치했는지 저장
    prev_action = None

    for action in actions:
        # 1. 국 종료 (다음 국으로 이동)
        if (
            action.get("type") == 1 and
            isinstance(action.get("result"), str) and
            (action["result"].startswith("Cg4ub") or action["result"].startswith("ChAub"))
        ):
            current_kyoku += 1
            riichi_declared_in_kyoku.clear()
            prev_action = None
            continue

        # 2. 론
        if (
            action.get("type") == 2 and
            action.get("userInput", {}).get("type") == 3 and
            action["userInput"].get("cpg", {}).get("type") == 9
        ):
            attacker = action["userInput"].get("seat", 0)
            defender = prev_action["userInput"].get("seat", 0) if prev_action else 0
            stats[attacker]["ron"].add(current_kyoku)
            stats[defender]["houju"].add(current_kyoku)

            # 다마텐: 리치 안 했고, 후로도 안 했으면
            if current_kyoku not in stats[attacker]["riichi"] and current_kyoku not in stats[attacker]["furo"]:
                stats[attacker]["dama"].add(current_kyoku)

        # 3. 쯔모
        if (
            action.get("type") == 2 and
            action.get("userInput", {}).get("operation", {}).get("type") == 8
        ):
            seat = action["userInput"].get("seat", 0)
            stats[seat]["tsumo"].add(current_kyoku)

            # 다마텐 체크
            if current_kyoku not in stats[seat]["riichi"] and current_kyoku not in stats[seat]["furo"]:
                stats[seat]["dama"].add(current_kyoku)

        # 4. 리치
        if (
            action.get("type") == 2 and
            action.get("userInput", {}).get("operation", {}).get("type") == 7
        ):
            seat = action["userInput"].get("seat", 0)

            # 추격 리치 조건: 이미 다른 사람이 리치한 경우
            if any(other_seat != seat for other_seat in riichi_declared_in_kyoku):
                stats[seat]["chase_riichi"].add(current_kyoku)

            stats[seat]["riichi"].add(current_kyoku)
            riichi_declared_in_kyoku.add(seat)

        # 5. 후로
        if (
            action.get("type") == 2 and
            action.get("userInput", {}).get("type") == 3 and
            action["userInput"].get("cpg", {}).get("type") in [2, 3, 5]
        ):
            seat = action["userInput"].get("seat", 0)
            stats[seat]["furo"].add(current_kyoku)

        if action["type"] != 1:
            prev_action = action

    # 요약 결과
    return {
        "total_kyoku": current_kyoku,
        "players": {
            seat: {
                "ron": len(stats[seat]["ron"]),
                "tsumo": len(stats[seat]["tsumo"]),
                "houju": len(stats[seat]["houju"]),
                "riichi": len(stats[seat]["riichi"]),
                "furo": len(stats[seat]["furo"]),
                "hora": len(stats[seat]["ron"] | stats[seat]["tsumo"]),
                "dama": len(stats[seat]["dama"]),
                "chase_riichi": len(stats[seat]["chase_riichi"])
            }
            for seat in range(4)
        }
    }

async def load_and_process_game_log2(lobby, uuid, version_to_force):
    logging.info("Loading game log")

    req = pb.ReqGameRecord()
    req.game_uuid = uuid
    req.client_version_string = f"web-{version_to_force}"
    res = await lobby.fetch_game_record(req)

    record_wrapper = pb.Wrapper()
    record_wrapper.ParseFromString(res.data)

    game_details = pb.GameDetailRecords()
    game_details.ParseFromString(record_wrapper.data)

    records = [action.result for action in game_details.actions if action.type == 1]

    game_records_count = len(records)
    logging.info("Found {} game records".format(game_records_count))

    hules = []
    round_record_wrapper = pb.Wrapper()

    for i in range(0, game_records_count):
        round_record_wrapper.ParseFromString(records[i])

        if round_record_wrapper.name == ".lq.RecordHule":
            record_hule = pb.RecordHule()
            record_hule.ParseFromString(round_record_wrapper.data)
            hule = MessageToDict(record_hule)["hules"][0]
            for fan in hule["fans"]:
                if "val" in fan:
                    fan_id = fan["id"]
                    fan_name = HAN.get(fan_id, f"알 수 없는 역({fan_id})")
                    hules.append([hule.get("seat", 0), fan_name])

    return res

def print_data_as_json(data, type):
    json = MessageToJson(data)
    logging.info("{} json {}".format(type, json))

if __name__ == "__main__":
    asyncio.run(main())



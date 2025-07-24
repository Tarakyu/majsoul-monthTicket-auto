import asyncio
import logging
import random
import uuid
import json
import aiohttp
import os
import gspread
import pytz
import base64

from datetime import datetime
from dotenv import load_dotenv
from ms.base import MSRPCChannel
from ms.rpc import Lobby
import ms.protocol_pb2 as pb
from google.protobuf.json_format import MessageToJson
from google.protobuf.json_format import MessageToDict
from oauth2client.service_account import ServiceAccountCredentials


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

    # reqPayMonthTicket = pb.ReqCommon()
    # resPayMonthTicket = await lobby.pay_month_ticket(reqPayMonthTicket)

    # print(resPayMonthTicket)

    # reqFetchMonthTicketInfo = pb.ReqCommon()
    # resFetchMonthTicketInfo = await lobby.fetch_month_ticket_info(reqFetchMonthTicketInfo)

    # print(resFetchMonthTicketInfo)

    #--
    # reqGameRecordList = pb.ReqGameRecordList()
    # resGameRecordList = await lobby.fetch_game_record_list(reqGameRecordList)

    # print(resGameRecordList)
    # dict_data = MessageToDict(resGameRecordList)
    # json_string = json.dumps(dict_data, ensure_ascii=False, indent=2)
    # with open("result.txt", "w", encoding="utf-8") as f:
    #     f.write(json_string)
    #---


    #--
    # fetchAccountStatisticInfo = pb.ReqAccountStatisticInfo(account_id=121659881)
    # resFetchAccountStatisticInfo = await lobby.fetch_account_statistic_info(fetchAccountStatisticInfo)

    # print(resFetchAccountStatisticInfo)
    # json_string = MessageToJson(resFetchAccountStatisticInfo)
    # with open("accountStatisticInfo.txt", "w", encoding="utf-8") as f:
    #     f.write(json_string)
    #--

    # await calculate_participants_statistics(lobby)

    # game_logs = await load_game_logs(lobby)
    # logging.info("Found {} records".format(len(game_logs)))
    # logging.info(game_logs)

    game_log = await load_and_process_game_log(lobby, "250724-f1b01f28-a857-4a8d-9738-99f6a9712320", version_to_force)
    logging.info("game {} result : \n{}".format(game_log.head.uuid, game_log.head.result))

    return True

# async def calculate_participants_statistics(lobby):
#     results = []
#     for account_id in account_nickname_map:
#         try:
#             request = pb.ReqAccountStatisticInfo(account_id=account_id)
#             response = await lobby.fetch_account_statistic_info(request)
#             parsed = MessageToDict(response)
#             stats_text = parse_statistics(account_id, parsed)
#             results.append(stats_text)
#         except Exception as e:
#             results.append(f"{account_nickname_map.get(account_id, account_id)}\n에러 발생: {e}\n")

#     results.append("---")
#     final_output = "\n".join(results)

#     with open("participants_statistics.txt", "w", encoding="utf-8") as f:
#         f.write(final_output)

async def load_and_process_game_log(lobby, uuid, version_to_force):
    logging.info("Loading game log")

    req = pb.ReqGameRecord()
    req.game_uuid = uuid
    req.client_version_string = f"web-{version_to_force}"
    res = await lobby.fetch_game_record(req)

    record_wrapper = pb.Wrapper()
    record_wrapper.ParseFromString(res.data)

    game_details = pb.GameDetailRecords()
    game_details.ParseFromString(record_wrapper.data)
    with open("result2.txt", "w", encoding="utf-8") as f:
        f.write(MessageToJson(game_details))

    result = analyze_game_log(MessageToDict(game_details))
    print(result)

    game_records_count = len(game_details.records)
    logging.info("Found {} game records".format(game_records_count))

    is_show_new_round_record = False
    is_show_discard_tile = False
    is_show_deal_tile = False

    for i in range(0, game_records_count):
        round_record_wrapper = game_details.actions[i]
        print(round_record_wrapper)

        if round_record_wrapper.name == ".lq.RecordNewRound" and not is_show_new_round_record:
            logging.info("Found record type = {}".format(round_record_wrapper.name))
            round_data = pb.RecordNewRound()
            round_data.ParseFromString(round_record_wrapper.data)
            print_data_as_json(round_data, "RecordNewRound")
            is_show_new_round_record = True

        if round_record_wrapper.name == ".lq.RecordDiscardTile" and not is_show_discard_tile:
            logging.info("Found record type = {}".format(round_record_wrapper.name))
            discard_tile = pb.RecordDiscardTile()
            discard_tile.ParseFromString(round_record_wrapper.data)
            print_data_as_json(discard_tile, "RecordDiscardTile")
            is_show_discard_tile = True

        if round_record_wrapper.name == ".lq.RecordDealTile" and not is_show_deal_tile:
            logging.info("Found record type = {}".format(round_record_wrapper.name))
            deal_tile = pb.RecordDealTile()
            deal_tile.ParseFromString(round_record_wrapper.data)
            print_data_as_json(deal_tile, "RecordDealTile")
            is_show_deal_tile = True

    return res

async def load_game_logs(lobby):
    logging.info("Loading game logs")

    records = []
    current = 1
    step = 30
    req = pb.ReqGameRecordList()
    req.start = current
    req.count = step
    res = await lobby.fetch_game_record_list(req)
    records.extend([r.uuid for r in res.record_list])

    return records


# def parse_statistics(account_id: int, data: dict) -> str:

#     nickname = account_nickname_map.get(account_id, str(account_id))
#     stat_data = data.get("statisticData", [])
#     for entry in stat_data:
#         if entry.get("mahjongCategory") == 1 and entry.get("gameCategory") == 4:
#             recent = entry.get("statistic", {}).get("recentRound", {})
#             total = recent.get("totalCount", 0)
#             rong = recent.get("rongCount", 0)
#             zimo = recent.get("zimoCount", 0)
#             fangchong = recent.get("fangchongCount", 0)
#             print(account_id, nickname, total, rong, zimo, fangchong)

#             if total > 0:
#                 hu_rate = (rong + zimo) / total * 100
#                 fc_rate = fangchong / total * 100
#                 return f"{nickname}\n화료율: {hu_rate:.0f}%\n방총률: {fc_rate:.0f}%\n"
#             else:
#                 return f"{nickname}\n화료율: -\n방총률: -\n"
#     return f"{nickname}\n데이터 없음\n"


def analyze_game_log(log_json: dict) -> dict:
    actions = log_json.get("actions", [])

    current_kyoku = 0
    stats = {
        seat: {
            "ron": set(),
            "tsumo": set(),
            "houju": set(),
            "riichi": set(),
            "furo": set()
        } for seat in range(4)
    }

    prev_action = None
    for action in actions:
        # 1. 국 종료 (다음 국으로 이동)
        if (
            action.get("type") == 1 and
            isinstance(action.get("result"), str) and
            (action["result"].startswith("Cg4ub") or action["result"].startswith("ChAub"))
        ):
            current_kyoku += 1
            prev_action = None
            continue

        # 2. 론 (cpg.type == 9)
        if (
            action.get("type") == 2 and
            action.get("userInput", {}).get("type") == 3 and
            action["userInput"].get("cpg", {}).get("type") == 9
        ):
            print(prev_action)
            attacker = action["userInput"].get("seat", 0)
            defender = prev_action["userInput"].get("seat", 0) if prev_action else 0
            stats[attacker]["ron"].add(current_kyoku)
            stats[defender]["houju"].add(current_kyoku)

        # 3. 쯔모 (operation.type == 8)
        if (
            action.get("type") == 2 and
            action.get("userInput", {}).get("operation", {}).get("type") == 8
        ):
            seat = action["userInput"].get("seat", 0)
            stats[seat]["tsumo"].add(current_kyoku)

        # 4. 리치 (operation.type == 7)
        if (
            action.get("type") == 2 and
            action.get("userInput", {}).get("operation", {}).get("type") == 7
        ):
            seat = action["userInput"].get("seat", 0)
            stats[seat]["riichi"].add(current_kyoku)

        # 5. 후로 (cpg.type in [2, 3, 5])
        if (
            action.get("type") == 2 and
            action.get("userInput", {}).get("type") == 3 and
            action["userInput"].get("cpg", {}).get("type") in [2, 3, 5]
        ):
            seat = action["userInput"].get("seat", 0)
            stats[seat]["furo"].add(current_kyoku)

        if (action["type"]) != 1:
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
                "hora": len(stats[seat]["ron"] | stats[seat]["tsumo"])
            }
            for seat in range(4)
        }
    }

if __name__ == "__main__":
    asyncio.run(main())

def print_data_as_json(data, type):
    json = MessageToJson(data)
    logging.info("{} json {}".format(type, json))

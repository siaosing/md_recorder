# -*- coding: utf-8 -*-
''''''
import thostmduserapi as mdapi
import thosttraderapi as api
from datetime import datetime, timedelta
import pandas as pd
import redis
import logging
import json
import time
import os
import zipfile

instrument_not_sub = []  # 不需要订阅的品种(如不活跃品种)
subID = []  # 订阅合约列表 通过查询tdapi来返回当天交易的所有合约
instrument_info = []  # 记录合约对应的交易所 合约乘数等添加到tick行情中，方便修改AveragePrice ActionDay等
PriceTick_dict = {}
ExchangeID_dict = {}
VolumeMultiple_dict = {}

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def load_json(filename: str) -> dict:
    """
    load data from json file in current path
    """
    with open(filename, mode='r', encoding='utf-8') as f:
        data = json.load(f)
    return data


# 连接交易前置所需信息 可以使用simnow
# simnow (simnow 没有bc cj等最新品种的合约信息)
TD_FrontAddr = "tcp://180.168.146.187:10100"  # simnow td_front
BROKERID = "9999"
USERID = ""
PASSWORD = ""
AppID = "simnow_client_test"
AuthCode = "0000000000000000"

# 也可以使用自己经过穿透式验证过的信息：
setting = load_json('setting.json')
TD_FrontAddr = setting['TD_FrontAddr']
BROKERID = setting['BROKERID']
USERID = setting["USERID"]
PASSWORD = setting["PASSWORD"]
AppID = setting["AppID"]
AuthCode = setting["AuthCode"]
MD_FrontAddr = setting["MD_FrontAddr"]
instrument_not_sub = setting["instrument_not_sub"]

# 行情前置 可以选任意一家期货公司公布的地址
# MD_FrontAddr = 'tcp://180.169.112.54:42213'  # hyqh
# MD_FrontAddr = 'tcp://114.80.225.2:41213'  # gmqh1
# MD_FrontAddr = 'tcp://180.169.112.54:41213'  # gmqh2
TRADINGDAY = ""  # 根据接口来返回交易日
qry_contract_finish = False


class CFtdcMdSpi(mdapi.CThostFtdcMdSpi):

    def __init__(self, tapi, redis_con):
        mdapi.CThostFtdcMdSpi.__init__(self)
        self.tapi = tapi
        self.red = redis_con

    def OnFrontConnected(self) -> None:
        logger.info('md OnFrontConnected')
        loginfield = mdapi.CThostFtdcReqUserLoginField()
        loginfield.BrokerID = BROKERID
        loginfield.UserID = USERID
        loginfield.Password = PASSWORD
        loginfield.UserProductInfo = "python dll"
        self.tapi.ReqUserLogin(loginfield, 0)

    def OnRspUserLogin(self, pRspUserLogin: 'CThostFtdcRspUserLoginField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool') -> None:
        logger.info(f"MD OnRspUserLogin, SessionID={pRspUserLogin.SessionID},ErrorID={pRspInfo.ErrorID},ErrorMsg={pRspInfo.ErrorMsg}")
        subIDs = [subID[i:i + 500] for i in range(0, len(subID), 500)]  # 分开订阅 每次最多订阅500个合约 防止合约数量过多订阅失败
        for i in range(len(subIDs)):
            self.tapi.SubscribeMarketData([id.encode('utf-8') for id in subIDs[i]], len(subIDs[i]))

    def OnRtnDepthMarketData(self, pDepthMarketData: 'CThostFtdcDepthMarketDataField') -> None:
        if not pDepthMarketData.UpdateTime:
            return
        if pDepthMarketData.Volume == 0:
            # logger.info(f'NULL tick:{pDepthMarketData.InstrumentID} {pDepthMarketData.UpdateTime} {pDepthMarketData.LastPrice} {pDepthMarketData.Volume}')
            return
        if not ('08:59:00' <= pDepthMarketData.UpdateTime <= '11:30:00' or '13:00:00' <= pDepthMarketData.UpdateTime <= '16:00:00'
                or pDepthMarketData.UpdateTime >= '20:59:00' or pDepthMarketData.UpdateTime <= '02:30:00'):
            # logger.info(f'Not trading tick:{pDepthMarketData.InstrumentID} {pDepthMarketData.UpdateTime} {pDepthMarketData.LastPrice} {pDepthMarketData.Volume}')
            return   # 过滤非交易时段tick

        now_stamp = datetime.now()
        today = now_stamp.strftime('%Y-%m-%d')
        tick_stamp = today + " " + pDepthMarketData.UpdateTime
        tick_stamp = datetime.strptime(tick_stamp, '%Y-%m-%d %H:%M:%S')
        delta = abs(now_stamp - tick_stamp)
        if delta.seconds > 60 * 60 and delta.seconds < 23 * 60 * 60:  # 与当前时间相距超过60分钟 无效tick扔掉
        # if delta > timedelta(minutes=10):
            logger.info(
                f"marketdata delay: ID: {pDepthMarketData.InstrumentID}, \
                LastPrice: {pDepthMarketData.LastPrice}, Volume: {pDepthMarketData.Volume}, \
                 Stamp: {pDepthMarketData.UpdateTime}, Now: {now_stamp.strftime('%H:%M:%S')}")
            return

        ExchangeID = ExchangeID_dict[pDepthMarketData.InstrumentID]
        VolumeMultiple = VolumeMultiple_dict[pDepthMarketData.InstrumentID]
        PriceTick = PriceTick_dict[pDepthMarketData.InstrumentID]
        mdlist = [TRADINGDAY,
                  pDepthMarketData.ActionDay,
                  pDepthMarketData.UpdateTime,
                  pDepthMarketData.UpdateMillisec,
                  pDepthMarketData.InstrumentID,
                  ExchangeID,
                  pDepthMarketData.LastPrice,
                  pDepthMarketData.PreSettlementPrice,
                  pDepthMarketData.PreClosePrice,
                  pDepthMarketData.PreOpenInterest,
                  pDepthMarketData.OpenPrice,
                  pDepthMarketData.HighestPrice,
                  pDepthMarketData.LowestPrice,
                  pDepthMarketData.Volume,
                  pDepthMarketData.Turnover,
                  pDepthMarketData.OpenInterest,
                  pDepthMarketData.ClosePrice,
                  pDepthMarketData.SettlementPrice,
                  pDepthMarketData.UpperLimitPrice,
                  pDepthMarketData.LowerLimitPrice,
                  pDepthMarketData.BidPrice1,
                  pDepthMarketData.BidVolume1,
                  pDepthMarketData.AskPrice1,
                  pDepthMarketData.AskVolume1,
                  pDepthMarketData.BidPrice2,
                  pDepthMarketData.BidVolume2,
                  pDepthMarketData.AskPrice2,
                  pDepthMarketData.AskVolume2,
                  pDepthMarketData.BidPrice3,
                  pDepthMarketData.BidVolume3,
                  pDepthMarketData.AskPrice3,
                  pDepthMarketData.AskVolume3,
                  pDepthMarketData.BidPrice4,
                  pDepthMarketData.BidVolume4,
                  pDepthMarketData.AskPrice4,
                  pDepthMarketData.AskVolume4,
                  pDepthMarketData.BidPrice5,
                  pDepthMarketData.BidVolume5,
                  pDepthMarketData.AskPrice5,
                  pDepthMarketData.AskVolume5,
                  pDepthMarketData.AveragePrice,
                  VolumeMultiple,
                  PriceTick]

        for i in range(len(mdlist)):  # 可以使用math.isclose来代替
            if ((isinstance(mdlist[i], int) or isinstance(mdlist[i], float)) and (abs(mdlist[i] - 1.7976931348623157e+308) < 0.000001)):
                mdlist[i] = -1
        self.red.lpush(pDepthMarketData.InstrumentID, ','.join([str(x) for x in mdlist]))

    def OnRspSubMarketData(self, pSpecificInstrument: 'CThostFtdcSpecificInstrumentField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool') -> None:
        if pRspInfo.ErrorID:
            logger.info("OnRspSubMarketData")
            logger.info(f"InstrumentID = {pSpecificInstrument.InstrumentID}, ErrorID = {pRspInfo.ErrorID}, ErrorMsg = {pRspInfo.ErrorMsg}")


class CTradeSpi(api.CThostFtdcTraderSpi):
    tapi = ''

    def __init__(self, tapi):
        api.CThostFtdcTraderSpi.__init__(self)
        self.tapi = tapi

    def OnFrontConnected(self) -> None:
        logger.info("TD OnFrontConnected")
        authfield = api.CThostFtdcReqAuthenticateField()
        authfield.BrokerID = BROKERID
        authfield.UserID = USERID
        authfield.AppID = AppID
        authfield.AuthCode = AuthCode
        self.tapi.ReqAuthenticate(authfield, 0)
        logger.info("send TD ReqAuthenticate ok")

    def OnRspAuthenticate(self, pRspAuthenticateField: 'CThostFtdcRspAuthenticateField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool') -> None:
        logger.info(f"BrokerID={pRspAuthenticateField.BrokerID}, ErrorID = {pRspInfo.ErrorID}, ErrorMsg = {pRspInfo.ErrorMsg}")

        if not pRspInfo.ErrorID:
            loginfield = api.CThostFtdcReqUserLoginField()
            loginfield.BrokerID = BROKERID
            loginfield.UserID = USERID
            loginfield.Password = PASSWORD
            loginfield.UserProductInfo = "python dll"
            self.tapi.ReqUserLogin(loginfield, 0)
            logger.info("send TD login ok")

    def OnRspUserLogin(self, pRspUserLogin: 'CThostFtdcRspUserLoginField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool') -> None:
        logger.info("OnRspUserLogin")
        logger.info(f"TradingDay={pRspUserLogin.TradingDay}, SessionID={pRspUserLogin.SessionID}, \
                    ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}")

        global TRADINGDAY
        TRADINGDAY = pRspUserLogin.TradingDay  # 接口返回交易日 不用自己根据日历判断

        qryinfofield = api.CThostFtdcQrySettlementInfoField()
        qryinfofield.BrokerID = BROKERID
        qryinfofield.InvestorID = USERID
        qryinfofield.TradingDay = pRspUserLogin.TradingDay  # 不填写的话默认查询最近一个交易日的结算单
        self.tapi.ReqQrySettlementInfo(qryinfofield, 0)
        logger.info("send TD ReqQrySettlementInfo ok")

    def OnRspQrySettlementInfo(self, pSettlementInfo: 'CThostFtdcSettlementInfoField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool') -> None:
        logger.info("OnRspQrySettlementInfo")
        if pSettlementInfo is not None:
            logger.info("content:", pSettlementInfo.Content)
        else:
            logger.warning("content null")
        pSettlementInfoConfirm = api.CThostFtdcSettlementInfoConfirmField()
        pSettlementInfoConfirm.BrokerID = BROKERID
        pSettlementInfoConfirm.InvestorID = USERID
        self.tapi.ReqSettlementInfoConfirm(pSettlementInfoConfirm, 0)
        logger.info("send TD ReqSettlementInfoConfirm ok")

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm: 'CThostFtdcSettlementInfoConfirmField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool') -> None:
        logger.info(f"OnRspSettlementInfoConfirm: ErrorID = {pRspInfo.ErrorID}, ErrorMsg = {pRspInfo.ErrorMsg}")

        # 结算单确认成功 发送合约查询请求
        v_req = api.CThostFtdcQryInstrumentField()  # 参数为空 默认返回所有交易合约
        self.tapi.ReqQryInstrument(v_req, 0)
        logger.info("send TD ReqQryInstrument ok")

    def OnRspQryInstrument(self, pInstrument: 'CThostFtdcInstrumentField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool') -> None:
        global subID, instrument_info, PriceTick_dict, ExchangeID_dict, VolumeMultiple_dict, qry_contract_finish
        if not bIsLast:
            instrument_temp = pInstrument.InstrumentID
            if 4 <= len(instrument_temp) <= 6 and instrument_temp[-1].isdigit():  # 过滤掉期权合约 只订阅期货合约
                if instrument_temp[:2] not in instrument_not_sub:
                    subID.append(instrument_temp)
                    instrument_info.append((pInstrument.InstrumentID, pInstrument.ExchangeID,
                                            pInstrument.ExchangeInstID, pInstrument.ProductID,
                                            pInstrument.VolumeMultiple, pInstrument.PriceTick))
                # ExchangeID等字段在深度行情中没有返回 此处记录下来备用

        if bIsLast:
            print('bIsLast:%s' % (bIsLast))
            if pRspInfo is not None:
                logger.warning('合约查询失败\n')
            else:
                logger.info('合约查询成功!')
                qry_contract_finish = True
                instrument_temp = pInstrument.InstrumentID
                if 4 <= len(instrument_temp) <= 6 and instrument_temp[-1].isdigit():
                    if instrument_temp[:2] not in instrument_not_sub:
                        subID.append(instrument_temp)
                        instrument_info.append((pInstrument.InstrumentID, pInstrument.ExchangeID,
                                                pInstrument.ExchangeInstID, pInstrument.ProductID,
                                                pInstrument.VolumeMultiple, pInstrument.PriceTick))
                logger.info('合约数:%d' % (len(subID)))
                instrument_info = pd.DataFrame(instrument_info, columns=['InstrumentID', 'ExchangeID', 'ExchangeInstID',
                                                                         'ProductID', 'VolumeMultiple', 'PriceTick'])
                PriceTick_dict = dict(zip(instrument_info.InstrumentID, instrument_info.PriceTick))
                ExchangeID_dict = dict(zip(instrument_info.InstrumentID, instrument_info.ExchangeID))
                VolumeMultiple_dict = dict(zip(instrument_info.InstrumentID, instrument_info.VolumeMultiple))
                # print(subID)


def save_redis(red):
    instruments = red.keys("*")
    if not os.path.exists('tick_data\\' + TRADINGDAY):
        try:
            os.makedirs('tick_data\\' + TRADINGDAY)
        except IOError:
            pass

    csvheader = ["TradingDay",
                 "ActionDay",
                 "UpdateTime",
                 "UpdateMillisec",
                 "InstrumentID",
                 "ExchangeID",
                 "LastPrice",
                 "PreSettlementPrice",
                 "PreClosePrice",
                 "PreOpenInterest",
                 "OpenPrice",
                 "HighestPrice",
                 "LowestPrice",
                 "Volume",
                 "Turnover",
                 "OpenInterest",
                 "ClosePrice",
                 "SettlementPrice",
                 "UpperLimitPrice",
                 "LowerLimitPrice",
                 "BidPrice1",
                 "BidVolume1",
                 "AskPrice1",
                 "AskVolume1",
                 "BidPrice2",
                 "BidVolume2",
                 "AskPrice2",
                 "AskVolume2",
                 "BidPrice3",
                 "BidVolume3",
                 "AskPrice3",
                 "AskVolume3",
                 "BidPrice4",
                 "BidVolume4",
                 "AskPrice4",
                 "AskVolume4",
                 "BidPrice5",
                 "BidVolume5",
                 "AskPrice5",
                 "AskVolume5",
                 "AveragePrice",
                 "VolumeMultiple",
                 "PriceTick"]

    # 循环遍历写一下
    for instrument in instruments:
        instrument = instrument.decode()
        csvname = 'tick_data' + os.path.sep + TRADINGDAY + os.path.sep + f"{instrument}.csv"
        isExist = os.path.isfile(csvname)

        b = red.lrange(instrument, 0, -1)[::-1]
        with open(csvname, 'a+') as f:
            f.write("\n".join([y.decode() for y in b]))

        if not isExist:
            csvfile = pd.read_csv(csvname, header=None)
            csvfile.to_csv(csvname, header=csvheader, index=False)
        else:
            csvfile = pd.read_csv(csvname)
            csvfile = csvfile.round(4)          # remove redundant 0s to save disk size
            csvfile.to_csv(csvname, index=False)

        red.delete(instrument)

    today = datetime.today().strftime('%Y%m%d')
    if today != TRADINGDAY:
        return
    now = datetime.now().strftime('%H:%M')
    if now < '15:00':
        return
    az = zipfile.ZipFile(".\\tick_data\\" + today + '.zip', mode='w', compression=0, allowZip64=True, compresslevel=None)
    for name_file in os.listdir(".\\tick_data\\" + today):
        az.write(filename=".\\tick_data\\" + today + "\\" + name_file, arcname=today + "\\" + name_file, compress_type=zipfile.ZIP_BZIP2, compresslevel=None)
        # os.remove(".\\" + today + "\\" + name_file)
    az.close()


def main():
    while True:
        log_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        file_handler = logging.FileHandler(f'logs/{log_timestamp}.log', mode='a', encoding='utf8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info('-----prepare to connect----------------')

        global subID, instrument_info, qry_contract_finish
        qry_contract_finish = False
        subID = []  # 订阅合约列表 通过查询tdapi来返回当天交易的所有合约
        instrument_info = []  # 记录合约对应的交易所 合约乘数等添加到tick行情中，方便修改AveragePrice ActionDay等

        tradeapi = api.CThostFtdcTraderApi_CreateFtdcTraderApi()  # 创建td_api
        tradespi = CTradeSpi(tradeapi)                            # 创建td_spi
        tradeapi.RegisterFront(TD_FrontAddr)                      # 注册交易前置地址
        tradeapi.RegisterSpi(tradespi)                            # 关联td_spi到td_api上
        tradeapi.SubscribePrivateTopic(api.THOST_TERT_QUICK)
        tradeapi.SubscribePublicTopic(api.THOST_TERT_QUICK)
        tradeapi.Init()                                           # 启动交易连接以查询合约
        while not qry_contract_finish:                            # 等待订阅所有合约完毕后释放tradeapi
            time.sleep(1)
        tradeapi.Release()  # 释放tradeapi线程，断开交易前置连接(同一个账户最多4个连接，超出后无法连接)

        mduserapi = mdapi.CThostFtdcMdApi_CreateFtdcMdApi()       # 创建md_api
        red = redis.Redis(db=0)                                  # 创建redis数据库
        mduserspi = CFtdcMdSpi(mduserapi, red)                    # 创建md_spi
        mduserapi.RegisterFront(MD_FrontAddr)                     # 注册行情前置地址
        mduserapi.RegisterSpi(mduserspi)                          # 关联md_spi到md_api上

        mduserapi.Init()  # 开始接收行情
        logger.info('-----start receiving market data-----')

        while True:
            now = datetime.now().strftime('%H:%M')
            if now == '16:00':
                mduserapi.Release()
                save_redis(red)
                logger.info('--------bye bye-----------------')
                time.sleep(60)
                os._exit(1)
                # logger.info('---save redis over, sleep till 20:30 to reconnect-----')
                # while True:
                #     time.sleep(10)
                #     if datetime.now().strftime('%H:%M') == '20:30':
                #         break
                # break
            if now == '02:35':
                mduserapi.Release()
                save_redis(red)
                logger.info('--------bye bye-----------------')
                time.sleep(60)
                os._exit(1)
                # logger.info('---night session over, sleep till 08:30 to reconnect-----')
                # while True:
                #     time.sleep(10)
                #     if datetime.now().strftime('%H:%M') == '08:30':
                #         break
                # break
            time.sleep(10)


if __name__ == '__main__':
    main()

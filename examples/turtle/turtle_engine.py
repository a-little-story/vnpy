from csv import DictReader
from collections import defaultdict
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt

from vnpy.trader.object import BarData

from turtle_strategy import TurtlePortfolio


class BacktestingEngine(object):
    """组合类CTA策略回测引擎"""
    
    def __init__(self):
        """Constructor"""
        self.portfolio = None
        
        # 合约配置信息
        self.vt_symbols = []
        self.sizes = {}                  # 合约大小字典
        self.priceticks = {}             # 最小价格变动字典
        self.variable_commissions = {}    # 变动手续费字典
        self.fixed_commissions = {}       # 固定手续费字典
        self.slippages = {}              # 滑点成本字典
        
        self.portfolio_value = 0
        self.start_dt = None
        self.end_dt = None
        self.current_dt = None
        
        self.data = {}
        self.trades = {}
        
        self.result = None
        self.results = []
    
    def set_period(self, start_dt: datetime, end_dt: datetime):
        """设置回测周期"""
        self.start_dt = start_dt
        self.end_dt = end_dt
    
    def init_portfolio(self, filename: str, portfolio_value: int = 10000000):
        """初始化投资组合"""
        self.portfolio_value = portfolio_value
        
        with open(filename) as f:
            reader = DictReader(f)
            
            for d in reader:
                vt_symbol = d["vt_symbol"]

                self.vt_symbols.append(vt_symbol)
                self.sizes[vt_symbol] = int(d["size"])
                self.priceticks[vt_symbol] = float(d["priceTick"])
                self.variable_commissions[vt_symbol] = float(d["variableCommission"])
                self.fixed_commissions[vt_symbol] = float(d["fixedCommission"])
                self.slippages[vt_symbol] = float(d["slippage"])
            
        self.portfolio = TurtlePortfolio(self)
        self.portfolio.init(portfolio_value, self.vt_symbols, self.sizes)
        
        self.output(f"投资组合的合约代码{self.vt_symbols}")
        self.output(f"投资组合的初始价值{portfolio_value}")
    
    def load_data(self):
        """加载数据"""
        mc = MongoClient()
        db = mc[DAILY_DB_NAME]
        
        for vt_symbol in self.vt_symbols:
            flt = {"datetime":{"$gte":self.start_dt,
                               "$lte":self.end_dt}} 
            
            collection = db[vt_symbol]
            cursor = collection.find(flt).sort("datetime")
            
            for d in cursor:
                bar = BarData()
                bar.__dict__ = d
                
                bars = self.data.setdefault(bar.datetime, {})
                bars[bar.vt_symbol] = bar
            
            self.output(u"%s数据加载完成，总数据量：%s" %(vt_symbol, cursor.count()))
        
        self.output(u"全部数据加载完成")
    
    def run_backtesting(self):
        """运行回测"""
        self.output(u"开始回放K线数据")
        
        for dt, bars in self.data.items():
            self.current_dt = dt
            
            previous_result = self.result
            
            self.result = DailyResult(dt)
            self.result.update_pos(self.portfolio.posDict)
            self.results.append(self.result)
            
            if previous_result:
                self.result.update_previous_close(previous_result.closes)
            
            for bar in bars.values():
                self.portfolio.on_bar(bar)
                self.result.update_bar(bar)
        
        self.output(u"K线数据回放结束")
    
    def calculate_result(self, annual_days=240):
        """计算结果"""
        self.output(u"开始统计回测结果")
        
        for result in self.results:
            result.calculate_pnl()
        
        results = self.results
        dates = [result.date for result in results]
        
        start_date = dates[0]
        end_date = dates[-1]  
        total_days = len(dates)
        
        profit_days = 0
        loss_days = 0
        end_balance = self.portfolio_value
        highlevel = self.portfolio_value
        total_net_pnl = 0
        total_commission = 0
        total_slippage = 0
        total_trade_count = 0
        
        netPnlList = []
        balanceList = []
        highlevelList = []
        drawdownList = []
        ddPercentList = []
        returnList = []
        
        for result in results:
            if result.netPnl > 0:
                profit_days += 1
            elif result.netPnl < 0:
                loss_days += 1
            netPnlList.append(result.netPnl)
            
            prevBalance = end_balance
            end_balance += result.netPnl
            balanceList.append(end_balance)
            returnList.append(end_balance/prevBalance - 1)
            
            highlevel = max(highlevel, end_balance)
            highlevelList.append(highlevel)
            
            drawdown = end_balance - highlevel
            drawdownList.append(drawdown)
            ddPercentList.append(drawdown/highlevel*100)
            
            total_commission += result.commission
            total_slippage += result.slippage
            total_trade_count += result.tradeCount
            total_net_pnl += result.netPnl
        
        maxDrawdown = min(drawdownList)
        maxDdPercent = min(ddPercentList)
        totalReturn = (end_balance / self.portfolio_value - 1) * 100
        dailyReturn = np.mean(returnList) * 100
        annualizedReturn = dailyReturn * annual_days
        returnStd = np.std(returnList) * 100
        
        if returnStd:
            sharpeRatio = dailyReturn / returnStd * np.sqrt(annual_days)
        else:
            sharpeRatio = 0
        
        # 返回结果
        result = {
            "start_date": start_date,
            "end_date": end_date,
            "total_days": total_days,
            "profit_days": profit_days,
            "loss_days": loss_days,
            "end_balance": end_balance,
            "maxDrawdown": maxDrawdown,
            "maxDdPercent": maxDdPercent,
            "total_net_pnl": total_net_pnl,
            "dailyNetPnl": total_net_pnl/total_days,
            "total_commission": total_commission,
            "dailyCommission": total_commission/total_days,
            "total_slippage": total_slippage,
            "dailySlippage": total_slippage/total_days,
            "total_trade_count": total_trade_count,
            "dailyTradeCount": total_trade_count/total_days,
            "totalReturn": totalReturn,
            "annualizedReturn": annualizedReturn,
            "dailyReturn": dailyReturn,
            "returnStd": returnStd,
            "sharpeRatio": sharpeRatio
            }
        
        timeseries = {
            "balance": balanceList,
            "return": returnList,
            "highLevel": highlevel,
            "drawdown": drawdownList,
            "ddPercent": ddPercentList,
            "date": dates,
            "netPnl": netPnlList
        }
        
        return timeseries, result
    
    
    def showResult(self):
        """显示回测结果"""
        timeseries, result = self.calculate_result()
        
        # 输出统计结果
        self.output("-" * 30)
        self.output(u"首个交易日：\t%s" % result["start_date"])
        self.output(u"最后交易日：\t%s" % result["end_date"])
        
        self.output(u"总交易日：\t%s" % result["total_days"])
        self.output(u"盈利交易日\t%s" % result["profit_days"])
        self.output(u"亏损交易日：\t%s" % result["loss_days"])
        
        self.output(u"起始资金：\t%s" % self.portfolio_value)
        self.output(u"结束资金：\t%s" % format_number(result["end_balance"]))
    
        self.output(u"总收益率：\t%s%%" % format_number(result["totalReturn"]))
        self.output(u"年化收益：\t%s%%" % format_number(result["annualizedReturn"]))
        self.output(u"总盈亏：\t%s" % format_number(result["total_net_pnl"]))
        self.output(u"最大回撤: \t%s" % format_number(result["maxDrawdown"]))   
        self.output(u"百分比最大回撤: %s%%" % format_number(result["maxDdPercent"]))   
        
        self.output(u"总手续费：\t%s" % format_number(result["total_commission"]))
        self.output(u"总滑点：\t%s" % format_number(result["total_slippage"]))
        self.output(u"总成交笔数：\t%s" % format_number(result["total_trade_count"]))
        
        self.output(u"日均盈亏：\t%s" % format_number(result["dailyNetPnl"]))
        self.output(u"日均手续费：\t%s" % format_number(result["dailyCommission"]))
        self.output(u"日均滑点：\t%s" % format_number(result["dailySlippage"]))
        self.output(u"日均成交笔数：\t%s" % format_number(result["dailyTradeCount"]))
        
        self.output(u"日均收益率：\t%s%%" % format_number(result["dailyReturn"]))
        self.output(u"收益标准差：\t%s%%" % format_number(result["returnStd"]))
        self.output(u"Sharpe Ratio：\t%s" % format_number(result["sharpeRatio"]))
        
        # 绘图
        fig = plt.figure(figsize=(10, 16))
        
        pBalance = plt.subplot(4, 1, 1)
        pBalance.set_title("Balance")
        plt.plot(timeseries["date"], timeseries["balance"])
        
        pDrawdown = plt.subplot(4, 1, 2)
        pDrawdown.set_title("Drawdown")
        pDrawdown.fill_between(range(len(timeseries["drawdown"])), timeseries["drawdown"])
        
        pPnl = plt.subplot(4, 1, 3)
        pPnl.set_title("Daily Pnl") 
        plt.bar(range(len(timeseries["drawdown"])), timeseries["netPnl"])

        pKDE = plt.subplot(4, 1, 4)
        pKDE.set_title("Daily Pnl Distribution")
        plt.hist(timeseries["netPnl"], bins=50)
        
        plt.show()        
    
    
    def sendOrder(self, vt_symbol, direction, offset, price, volume):
        """记录交易数据（由portfolio调用）"""
        # 对价格四舍五入
        priceTick = self.priceticks[vt_symbol]
        price = int(round(price/priceTick, 0)) * priceTick
        
        # 记录成交数据
        trade = TradeData(vt_symbol, direction, offset, price, volume)
        l = self.trades.setdefault(self.current_dt, [])        
        l.append(trade)
        
        self.result.updateTrade(trade)

    
    def output(self, content):
        """输出信息"""
        print(content)
    
    
    def getTradeData(self, vt_symbol=""):
        """获取交易数据"""
        tradeList = []
        
        for l in self.trades.values():
            for trade in l:
                if not vt_symbol:
                    tradeList.append(trade)
                elif trade.vt_symbol == vt_symbol:
                    tradeList.append(trade)
        
        return tradeList

   
class TradeData(object):
    """"""
   
    def __init__(self, vt_symbol, direction, offset, price, volume):
        """Constructor"""
        self.vt_symbol = vt_symbol
        self.direction = direction
        self.offset = offset
        self.price = price
        self.volume = volume


class DailyResult(object):
    """每日的成交记录"""
    
    def __init__(self, date):
        """Constructor"""
        self.date = date
        
        self.closes = {}                     # 收盘价字典
        self.previousCloseDict = {}             # 昨收盘字典
        
        self.trades = defaultdict(list)      # 成交字典
        self.posDict = {}                       # 持仓字典（开盘时）
        
        self.tradingPnl = 0                     # 交易盈亏
        self.holdingPnl = 0                     # 持仓盈亏
        self.totalPnl = 0                       # 总盈亏
        self.commission = 0                     # 佣金
        self.slippage = 0                       # 滑点
        self.netPnl = 0                         # 净盈亏
        self.tradeCount = 0                     # 成交笔
    
    def updateTrade(self, trade):
        """更新交易"""
        l = self.trades[trade.vt_symbol]
        l.append(trade)
        self.tradeCount += 1
        
    def update_pos(self, d):
        """更新昨持仓"""
        self.posDict.update(d)
    
    def update_bar(self, bar):
        """更新K线"""
        self.closes[bar.vt_symbol] = bar.close
    
    def update_previous_close(self, d):
        """更新昨收盘"""
        self.previousCloseDict.update(d)

    def calculate_trading_pnl(self):
        """计算当日交易盈亏"""
        for vt_symbol, l in self.trades.items():
            close = self.closes[vt_symbol]
            size = self.sizes[vt_symbol]
            
            slippage = self.slippages[vt_symbol]
            variableCommission = self.variable_commissions[vt_symbol]
            fixedCommission = self.fixed_commissions[vt_symbol]
            
            for trade in l:
                if trade.direction == Direction.LONG:
                    side = 1
                else:
                    side = -1
                
                commissionCost = (trade.volume * fixedCommission + 
                                  trade.volume * trade.price * variableCommission)
                slippageCost = trade.volume * slippage
                pnl = (close - trade.price) * trade.volume * side * size
                
                self.commission += commissionCost
                self.slippage += slippageCost
                self.tradingPnl += pnl
    
    def calculate_holding_pnl(self):
        """计算当日持仓盈亏"""
        for vt_symbol, pos in self.posDict.items():
            previousClose = self.previousCloseDict.get(vt_symbol, 0)
            close = self.closes[vt_symbol]
            size = self.sizes[vt_symbol]
            
            pnl = (close - previousClose) * pos * size
            self.holdingPnl += pnl

    def calculate_pnl(self):
        """计算总盈亏"""
        self.calculate_holding_pnl()
        self.calculate_trading_pnl()
        self.totalPnl = self.holdingPnl + self.tradingPnl
        self.netPnl = self.totalPnl - self.commission - self.slippage



def format_number(n):
    """格式化数字到字符串"""
    rn = round(n, 2)        # 保留两位小数
    return format(rn, ",")  # 加上千分符

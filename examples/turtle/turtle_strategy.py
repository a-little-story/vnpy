from collections import defaultdict

from vnpy.trader.object import BarData
from vnpy.trader.constant import Direction, Offset
from vnpy.trader.utility import ArrayManager


MAX_PRODUCT_POS = 4         # 单品种最大持仓
MAX_DIRECTION_POS = 10      # 单方向最大持仓


class TurtleResult(object):
    """一次完整的开平交易"""

    def __init__(self):
        """Constructor"""
        self.unit = 0
        self.entry = 0                  # 开仓均价
        self.exit = 0                   # 平仓均价
        self.pnl = 0                    # 盈亏

    def open(self, price: float, change: int):
        """开仓或者加仓"""
        cost = self.unit * self.entry    # 计算之前的开仓成本
        cost += change * price          # 加上新仓位的成本
        self.unit += change              # 加上新仓位的数量
        self.entry = cost / self.unit    # 计算新的平均开仓成本

    def close(self, price: float):
        """平仓"""
        self.exit = price
        self.pnl = self.unit * (self.exit - self.entry)


class TurtleSignal(object):
    """海龟信号"""

    def __init__(
        self,
        portfolio: "TurtlePortfolio",
        vt_symbol: str,
        entry_window: int,
        exit_window: int,
        atr_window: int,
        profit_check=False
    ):
        """Constructor"""
        self.portfolio = portfolio          # 投资组合

        self.vt_symbol = vt_symbol          # 合约代码
        self.entry_window = entry_window    # 入场通道周期数
        self.exit_window = exit_window      # 出场通道周期数
        self.atr_window = atr_window        # 计算ATR周期数
        self.profit_check = profit_check    # 是否检查上一笔盈利

        self.am = ArrayManager(60)          # K线容器

        self.atr_volatility = 0             # ATR波动率
        self.entry_up = 0                   # 入场通道
        self.entry_down = 0
        self.exit_up = 0                    # 出场通道
        self.exit_down = 0

        self.long_entry_1 = 0               # 多头入场位
        self.long_entry_2 = 0
        self.long_entry_3 = 0
        self.long_entry_4 = 0
        self.long_stop = 0                  # 多头止损位

        self.short_entry_1 = 0              # 空头入场位
        self.short_entry_2 = 0
        self.short_entry_3 = 0
        self.short_entry_4 = 0
        self.short_stop = 0                 # 空头止损位

        self.unit = 0                       # 信号持仓
        self.last_result = None             # 当前的交易
        self.results = []                   # 交易列表
        self.last_bar = None                # 最新K线

    def on_bar(self, bar: BarData):
        """"""
        self.last_bar = bar

        self.am.update_bar(bar)
        if not self.am.inited:
            return

        self.generate_signal(bar)
        self.calculate_indicator()

    def generate_signal(self, bar: BarData):
        """
        判断交易信号
        要注意在任何一个数据点：buy/sell/short/cover只允许执行一类动作
        """
        # 如果指标尚未初始化，则忽略
        if not self.long_entry_1:
            return

        # 优先检查平仓
        if self.unit > 0:
            long_exit = max(self.long_stop, self.exit_down)

            if bar.low <= long_exit:
                self.sell(long_exit)
                return
        elif self.unit < 0:
            short_exit = min(self.short_stop, self.exit_up)
            if bar.high >= short_exit:
                self.cover(short_exit)
                return

        # 没有仓位或者持有多头仓位的时候，可以做多（加仓）
        if self.unit >= 0:
            trade = False

            if bar.high >= self.long_entry_1 and self.unit < 1:
                self.buy(self.long_entry_1, 1)
                trade = True

            if bar.high >= self.long_entry_2 and self.unit < 2:
                self.buy(self.long_entry_2, 1)
                trade = True

            if bar.high >= self.long_entry_3 and self.unit < 3:
                self.buy(self.long_entry_3, 1)
                trade = True

            if bar.high >= self.long_entry_4 and self.unit < 4:
                self.buy(self.long_entry_4, 1)
                trade = True

            if trade:
                return

        # 没有仓位或者持有空头仓位的时候，可以做空（加仓）
        if self.unit <= 0:
            if bar.low <= self.short_entry_1 and self.unit > -1:
                self.short(self.short_entry_1, 1)

            if bar.low <= self.short_entry_2 and self.unit > -2:
                self.short(self.short_entry_2, 1)

            if bar.low <= self.short_entry_3 and self.unit > -3:
                self.short(self.short_entry_3, 1)

            if bar.low <= self.short_entry_4 and self.unit > -4:
                self.short(self.short_entry_4, 1)

    def calculate_indicator(self):
        """计算技术指标"""
        self.entry_up, self.entry_down = self.am.donchian(self.entry_window)
        self.exit_up, self.exit_down = self.am.donchian(self.exit_window)

        # 有持仓后，ATR波动率和入场位等都不再变化
        if not self.unit:
            self.atr_volatility = self.am.atr(self.atr_window)

            self.long_entry_1 = self.entry_up
            self.long_entry_2 = self.entry_up + self.atr_volatility * 0.5
            self.long_entry_3 = self.entry_up + self.atr_volatility * 1
            self.long_entry_4 = self.entry_up + self.atr_volatility * 1.5
            self.long_stop = 0

            self.short_entry_1 = self.entry_down
            self.short_entry_2 = self.entry_down - self.atr_volatility * 0.5
            self.short_entry_3 = self.entry_down - self.atr_volatility * 1
            self.short_entry_4 = self.entry_down - self.atr_volatility * 1.5
            self.short_stop = 0

    def new_signal(self, direction: Direction, offset: Offset, price: float, volume: int):
        """"""
        self.portfolio.new_signal(self, direction, offset, price, volume)

    def buy(self, price: float, volume: int):
        """买入开仓"""
        price = self.calculate_trade_price(Direction.LONG, price)

        self.open(price, volume)
        self.new_signal(Direction.LONG, Offset.OPEN, price, volume)

        # 以最后一次加仓价格，加上两倍N计算止损
        self.long_stop = price - self.atr_volatility * 2

    def sell(self, price: float):
        """卖出平仓"""
        price = self.calculate_trade_price(Direction.SHORT, price)
        volume = abs(self.unit)

        self.close(price)
        self.new_signal(Direction.SHORT, Offset.CLOSE, price, volume)

    def short(self, price: float, volume: int):
        """卖出开仓"""
        price = self.calculate_trade_price(Direction.SHORT, price)

        self.open(price, -volume)
        self.new_signal(Direction.SHORT, Offset.OPEN, price, volume)

        # 以最后一次加仓价格，加上两倍N计算止损
        self.short_stop = price + self.atr_volatility * 2

    def cover(self, price: float):
        """买入平仓"""
        price = self.calculate_trade_price(Direction.LONG, price)
        volume = abs(self.unit)

        self.close(price)
        self.new_signal(Direction.LONG, Offset.CLOSE, price, volume)

    def open(self, price: float, change: int):
        """开仓"""
        self.unit += change

        if not self.last_result:
            self.last_result = TurtleResult()
        self.last_result.open(price, change)

    def close(self, price: float):
        """平仓"""
        self.unit = 0

        self.last_result.close(price)
        self.results.append(self.last_result)
        self.last_result = None

    def get_last_pnl(self):
        """获取上一笔交易的盈亏"""
        if not self.results:
            return 0

        result = self.results[-1]
        return result.pnl

    def calculate_trade_price(self, direction: Direction, price: float):
        """计算成交价格"""
        # 买入时，停止单成交的最优价格不能低于当前K线开盘价
        if direction == Direction.LONG:
            trade_price = max(self.last_bar.open, price)
        # 卖出时，停止单成交的最优价格不能高于当前K线开盘价
        else:
            trade_price = min(self.last_bar.open, price)

        return trade_price


class TurtlePortfolio(object):
    """海龟组合"""

    def __init__(self, engine):
        """Constructor"""
        self.engine = engine

        self.signals = defaultdict(list)

        self.units = {}             # 每个品种的持仓情况
        self.total_long = 0         # 总的多头持仓
        self.total_short = 0        # 总的空头持仓

        self.trading_signals = {}   # 交易中的信号字典

        self.sizes = {}             # 合约大小字典
        self.multipliers = {}       # 按照波动幅度计算的委托量单位字典
        self.poses = {}             # 真实持仓量字典

        self.portfolio_value = 0    # 组合市值

    def init(self, portfolio_value: float, vt_symbols: list, sizes: dict):
        """"""
        self.portfolio_value = portfolio_value
        self.sizes = sizes

        for vt_symbol in vt_symbols:
            signal_1 = TurtleSignal(self, vt_symbol, 20, 10, 20, True)
            signal_2 = TurtleSignal(self, vt_symbol, 55, 20, 20, False)

            signal_list = self.signals[vt_symbol]
            signal_list.append(signal_1)
            signal_list.append(signal_2)

            self.units[vt_symbol] = 0
            self.poses[vt_symbol] = 0

    def on_bar(self, bar):
        """"""
        for signal in self.signals[bar.vt_symbol]:
            signal.on_bar(bar)

    def new_signal(self, signal: TurtleSignal, direction: Direction,
                   offset: Offset, price: float, volume: int):
        """对交易信号进行过滤，符合条件的才发单执行"""
        unit = self.units[signal.vt_symbol]

        # 如果当前无仓位，则重新根据波动幅度计算委托量单位
        if not unit:
            size = self.sizes[signal.vt_symbol]
            risk_value = self.portfolio_value * 0.01

            multiplier = risk_value / (signal.atr_volatility * size)
            multiplier = int(round(multiplier, 0))
            self.multipliers[signal.vt_symbol] = multiplier
        else:
            multiplier = self.multipliers[signal.vt_symbol]

        # 开仓
        if offset == Offset.OPEN:
            # 检查上一次是否为盈利
            if signal.profit_check:
                pnl = signal.get_last_pnl()
                if pnl > 0:
                    return

            # 买入
            if direction == Direction.LONG:
                # 组合持仓不能超过上限
                if self.total_long >= MAX_DIRECTION_POS:
                    return

                # 单品种持仓不能超过上限
                if self.units[signal.vt_symbol] >= MAX_PRODUCT_POS:
                    return
            # 卖出
            else:
                if self.total_short <= -MAX_DIRECTION_POS:
                    return

                if self.units[signal.vt_symbol] <= -MAX_PRODUCT_POS:
                    return
        # 平仓
        else:
            if direction == Direction.LONG:
                # 必须有空头持仓
                if unit >= 0:
                    return

                # 平仓数量不能超过空头持仓
                volume = min(volume, abs(unit))
            else:
                if unit <= 0:
                    return

                volume = min(volume, abs(unit))

        # 获取当前交易中的信号，如果不是本信号，则忽略
        current_signal = self.trading_signals.get(signal.vt_symbol, None)
        if current_signal and current_signal is not signal:
            return

        # 开仓则缓存该信号的交易状态
        if offset == Offset.OPEN:
            self.trading_signals[signal.vt_symbol] = signal
        # 平仓则清除该信号
        else:
            self.trading_signals.pop(signal.vt_symbol)

        self.send_order(signal.vt_symbol, direction,
                        offset, price, volume, multiplier)

    def send_order(self, vt_symbol: str, direction: Direction, offset: Offset,
                   price: float, volume: int, multiplier: int):
        """"""
        # 计算合约持仓
        if direction == Direction.LONG:
            self.units[vt_symbol] += volume
            self.poses[vt_symbol] += volume * multiplier
        else:
            self.units[vt_symbol] -= volume
            self.poses[vt_symbol] -= volume * multiplier

        # 计算总持仓
        self.total_long = 0
        self.total_short = 0

        for unit in self.units.values():
            if unit > 0:
                self.total_long += unit
            elif unit < 0:
                self.total_short += unit

        # 向回测引擎中发单记录
        self.engine.send_order(vt_symbol, direction, offset,
                               price, volume * multiplier)

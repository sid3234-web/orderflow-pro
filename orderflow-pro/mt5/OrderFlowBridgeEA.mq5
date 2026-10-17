#property strict
#property version   "1.10"
#property description "OrderFlow Pro signal bridge - manual approval, demo-only"

#include <Trade/Trade.mqh>

input string InpSignalUrl = "https://orderflow-pro-4m31.onrender.com/api/mt5-signal/BTCUSDT";
input string InpTradeSymbol = "BTCUSD";
input double InpLots = 0.01;
input bool   InpEnableDemoOrders = false; // Safety lock: false by default
input int    InpPollSeconds = 5;
input int    InpATRPeriod = 14;
input double InpStopLossATR = 2.0;
input double InpTakeProfitR = 1.5;
input ulong  InpMagicNumber = 26101701;

CTrade trade;
int atrHandle = INVALID_HANDLE;
string buttonName = "OFP_MANUAL_APPROVE";
string pendingSide = "WAIT";
string pendingSignalId = "";
string consumedSignalId = "";
int pendingScore = 0;

string FieldValue(const string response, const string key)
{
   string needle = key + "=";
   int start = StringFind(response, needle);
   if(start < 0) return "";
   start += StringLen(needle);
   int finish = StringFind(response, "\n", start);
   if(finish < 0) finish = StringLen(response);
   string value = StringSubstr(response, start, finish - start);
   StringReplace(value, "\r", "");
   StringReplace(value, "\n", "");
   return value;
}

void RemoveApprovalButton()
{
   if(ObjectFind(0, buttonName) >= 0)
      ObjectDelete(0, buttonName);
}

void ShowApprovalButton()
{
   if(ObjectFind(0, buttonName) < 0)
   {
      ObjectCreate(0, buttonName, OBJ_BUTTON, 0, 0, 0);
      ObjectSetInteger(0, buttonName, OBJPROP_CORNER, CORNER_LEFT_UPPER);
      ObjectSetInteger(0, buttonName, OBJPROP_XDISTANCE, 15);
      ObjectSetInteger(0, buttonName, OBJPROP_YDISTANCE, 25);
      ObjectSetInteger(0, buttonName, OBJPROP_XSIZE, 260);
      ObjectSetInteger(0, buttonName, OBJPROP_YSIZE, 34);
      ObjectSetInteger(0, buttonName, OBJPROP_SELECTABLE, true);
      ObjectSetInteger(0, buttonName, OBJPROP_HIDDEN, false);
      ObjectSetInteger(0, buttonName, OBJPROP_FONTSIZE, 10);
   }
   string label = "APPROVE DEMO " + pendingSide + " | score " + IntegerToString(pendingScore);
   if(!InpEnableDemoOrders) label = "SIGNAL " + pendingSide + " | EXECUTION LOCKED";
   ObjectSetString(0, buttonName, OBJPROP_TEXT, label);
   ObjectSetInteger(0, buttonName, OBJPROP_BGCOLOR,
                    pendingSide == "BUY" ? clrDarkGreen : clrMaroon);
   ObjectSetInteger(0, buttonName, OBJPROP_COLOR, clrWhite);
   ChartRedraw(0);
}

bool PollSignal()
{
   char requestData[];
   char responseData[];
   string responseHeaders;
   ArrayResize(requestData, 0);

   ResetLastError();
   int status = WebRequest("GET", InpSignalUrl, "", 5000,
                           requestData, responseData, responseHeaders);
   if(status == -1)
   {
      Print("OrderFlow Pro: WebRequest failed. Error ", GetLastError(),
            ". Add https://orderflow-pro-4m31.onrender.com to MT5 Tools > Options > Expert Advisors > allowed WebRequest URLs.");
      return false;
   }
   if(status != 200)
   {
      Print("OrderFlow Pro: signal endpoint HTTP status ", status,
            ". Check that the Render deployment has finished.");
      return false;
   }

   string response = CharArrayToString(responseData, 0, -1, CP_UTF8);
   string side = FieldValue(response, "side");
   string eligible = FieldValue(response, "eligible");
   string signalId = FieldValue(response, "signal_id");
   string scoreText = FieldValue(response, "score");
   string execution = FieldValue(response, "execution");

   if(execution != "disabled")
   {
      Print("OrderFlow Pro: unexpected endpoint response; refusing to continue.");
      return false;
   }

   if(eligible != "true" || (side != "BUY" && side != "SELL") || signalId == "")
   {
      pendingSide = "WAIT";
      pendingSignalId = "";
      RemoveApprovalButton();
      Comment("OrderFlow Pro bridge: connected\nSignal: WAIT / not eligible\nNo order sent.");
      return true;
   }

   if(signalId != pendingSignalId)
   {
      pendingSignalId = signalId;
      pendingSide = side;
      pendingScore = (int)StringToInteger(scoreText);
      if(pendingSignalId != consumedSignalId)
      {
         ShowApprovalButton();
         Print("OrderFlow Pro signal ready: ", side, " score=", pendingScore,
               ". No order sent; manual approval is required.");
      }
   }
   else
   {
      pendingSide = side;
      pendingScore = (int)StringToInteger(scoreText);
      if(pendingSignalId != consumedSignalId) ShowApprovalButton();
   }

   Comment("OrderFlow Pro bridge: connected\nSymbol: ", InpDashboardSymbol,
           "\nSignal: ", pendingSide, " | score ", pendingScore,
           "\nExecution lock: ", (InpEnableDemoOrders ? "demo orders enabled; click to approve" : "ON"),
           "\nNo trade is sent without clicking the chart button.");
   return true;
}

bool SendApprovedDemoOrder()
{
   if(!InpEnableDemoOrders)
   {
      Alert("OrderFlow Pro execution is locked. InpEnableDemoOrders is false.");
      Print("Manual click received, but execution remains disabled by safety input.");
      return false;
   }

   if((ENUM_ACCOUNT_TRADE_MODE)AccountInfoInteger(ACCOUNT_TRADE_MODE) != ACCOUNT_TRADE_MODE_DEMO)
   {
      Alert("Safety lock: this EA only permits demo accounts.");
      Print("Order refused: account is not reported as DEMO.");
      return false;
   }

   if(pendingSignalId == "" || (pendingSide != "BUY" && pendingSide != "SELL"))
   {
      Alert("No eligible OrderFlow Pro signal is waiting for approval.");
      return false;
   }
   if(pendingSignalId == consumedSignalId)
   {
      Alert("This signal has already been used.");
      return false;
   }

   string symbol = InpTradeSymbol;
   if(!SymbolSelect(symbol, true))
   {
      Alert("Cannot select broker symbol: ", symbol);
      return false;
   }

   double minVol = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double maxVol = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double stepVol = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
   if(InpLots < minVol || InpLots > maxVol || stepVol <= 0.0 ||
      MathAbs(MathRound(InpLots / stepVol) * stepVol - InpLots) > 0.0000001)
   {
      Alert("Lot size rejected by broker symbol rules. Check Symbol Specification.");
      Print("Lot validation failed. Requested=", InpLots, " min=", minVol,
            " max=", maxVol, " step=", stepVol);
      return false;
   }

   if(atrHandle == INVALID_HANDLE)
   {
      Alert("ATR indicator is not available.");
      return false;
   }
   double atrBuffer[];
   ArraySetAsSeries(atrBuffer, true);
   if(CopyBuffer(atrHandle, 0, 0, 1, atrBuffer) != 1 || atrBuffer[0] <= 0.0)
   {
      Alert("Waiting for valid ATR data. No order sent.");
      return false;
   }

   MqlTick tick;
   if(!SymbolInfoTick(symbol, tick) || tick.ask <= 0.0 || tick.bid <= 0.0)
   {
      Alert("No valid broker price available.");
      return false;
   }

   double spread = tick.ask - tick.bid;
   if(spread > atrBuffer[0] * 0.20)
   {
      Alert("Spread is too large relative to ATR. No order sent.");
      Print("Spread filter blocked order. Spread=", spread, " ATR=", atrBuffer[0]);
      return false;
   }

   double price = pendingSide == "BUY" ? tick.ask : tick.bid;
   double riskDistance = atrBuffer[0] * InpStopLossATR;
   double sl = pendingSide == "BUY" ? price - riskDistance : price + riskDistance;
   double tpDistance = riskDistance * InpTakeProfitR;
   double tp = pendingSide == "BUY" ? price + tpDistance : price - tpDistance;

   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   long stopsLevel = SymbolInfoInteger(symbol, SYMBOL_TRADE_STOPS_LEVEL);
   if(point <= 0.0 || riskDistance < (double)stopsLevel * point ||
      tpDistance < (double)stopsLevel * point)
   {
      Alert("Broker stop-distance rules block this order. No order sent.");
      return false;
   }
   sl = NormalizeDouble(sl, digits);
   tp = NormalizeDouble(tp, digits);

   trade.SetExpertMagicNumber(InpMagicNumber);
   trade.SetDeviationInPoints(100);
   if(!trade.SetTypeFillingBySymbol(symbol))
   {
      Print("Could not set broker filling mode for ", symbol);
      return false;
   }

   bool sent = false;
   if(pendingSide == "BUY")
      sent = trade.Buy(InpLots, symbol, 0.0, sl, tp, "OrderFlow Pro manual demo approval");
   else
      sent = trade.Sell(InpLots, symbol, 0.0, sl, tp, "OrderFlow Pro manual demo approval");

   if(!sent)
   {
      Alert("Order rejected: ", trade.ResultRetcodeDescription());
      Print("Order failed. Retcode=", trade.ResultRetcode(),
            " description=", trade.ResultRetcodeDescription());
      return false;
   }

   consumedSignalId = pendingSignalId;
   RemoveApprovalButton();
   Print("DEMO order sent after manual chart approval. Side=", pendingSide,
         " symbol=", symbol, " volume=", InpLots, " SL=", sl, " TP=", tp,
         " ticket=", trade.ResultOrder());
   Alert("Demo order submitted: ", pendingSide, " ", symbol,
         ". Verify it in the MT5 Trade tab.");
   return true;
}

int OnInit()
{
   if(InpPollSeconds < 2)
   {
      Print("Poll interval must be at least 2 seconds.");
      return INIT_PARAMETERS_INCORRECT;
   }
   atrHandle = iATR(InpTradeSymbol, PERIOD_M1, InpATRPeriod);
   if(atrHandle == INVALID_HANDLE)
      Print("ATR initialization failed for ", InpTradeSymbol,
            ". Order placement will be blocked until valid ATR is available.");

   EventSetTimer(InpPollSeconds);
   trade.SetExpertMagicNumber(InpMagicNumber);
   Print("OrderFlow Pro bridge initialized. Dashboard signals only; manual approval required.");
   Print("Demo order execution default is ", (InpEnableDemoOrders ? "ENABLED" : "LOCKED"),
         ". No automatic orders are sent.");
   PollSignal();
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   if(atrHandle != INVALID_HANDLE) IndicatorRelease(atrHandle);
   RemoveApprovalButton();
   Comment("");
   Print("OrderFlow Pro bridge stopped.");
}

void OnTimer()
{
   PollSignal();
}

void OnTick()
{
   // Signal polling runs on a timer. Orders can only be requested by a chart-button click.
}

void OnChartEvent(const int id, const long &lparam, const double &dparam, const string &sparam)
{
   if(id == CHARTEVENT_OBJECT_CLICK && sparam == buttonName)
   {
      ObjectSetInteger(0, buttonName, OBJPROP_STATE, false);
      SendApprovedDemoOrder();
      ChartRedraw(0);
   }
}

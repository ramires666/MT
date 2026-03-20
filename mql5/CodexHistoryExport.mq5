#property strict
#property script_show_inputs

input string InpJobFile = "Codex\\history_job.txt";
input string InpStatusFile = "Codex\\history_status.txt";

ENUM_TIMEFRAMES ParseTimeframe(const string value)
{
   if(value == "M1")  return PERIOD_M1;
   if(value == "M5")  return PERIOD_M5;
   if(value == "M15") return PERIOD_M15;
   if(value == "M30") return PERIOD_M30;
   if(value == "H1")  return PERIOD_H1;
   if(value == "H4")  return PERIOD_H4;
   if(value == "D1")  return PERIOD_D1;
   return PERIOD_CURRENT;
}

void WriteStatus(const int handle,const string symbol,const string status,const string detail,const int bars)
{
   FileWriteString(handle, symbol + "|" + status + "|" + IntegerToString(bars) + "|" + detail + "\n");
}

bool ExportRates(const string symbol,const string timeframe_text,const datetime from_ts,const datetime to_ts,const string output_file,const int status_handle)
{
   ENUM_TIMEFRAMES timeframe = ParseTimeframe(timeframe_text);
   if(timeframe == PERIOD_CURRENT)
   {
      WriteStatus(status_handle, symbol, "error", "unsupported timeframe", 0);
      return false;
   }

   SymbolSelect(symbol, true);
   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   ResetLastError();
   int copied = CopyRates(symbol, timeframe, from_ts, to_ts, rates);
   if(copied <= 0)
   {
      WriteStatus(status_handle, symbol, "error", "CopyRates failed: " + IntegerToString(GetLastError()), copied);
      return false;
   }

   int handle = FileOpen(output_file, FILE_COMMON | FILE_BIN | FILE_WRITE);
   if(handle == INVALID_HANDLE)
   {
      WriteStatus(status_handle, symbol, "error", "FileOpen failed: " + IntegerToString(GetLastError()), copied);
      return false;
   }

   FileWriteString(handle, "CODX", 4);
   FileWriteInteger(handle, 1, INT_VALUE);
   FileWriteInteger(handle, copied, INT_VALUE);

   for(int i = 0; i < copied; i++)
   {
      FileWriteLong(handle, (long)rates[i].time);
      FileWriteDouble(handle, rates[i].open);
      FileWriteDouble(handle, rates[i].high);
      FileWriteDouble(handle, rates[i].low);
      FileWriteDouble(handle, rates[i].close);
      FileWriteLong(handle, (long)rates[i].tick_volume);
      FileWriteInteger(handle, (int)rates[i].spread, INT_VALUE);
      FileWriteLong(handle, (long)rates[i].real_volume);
   }

   FileClose(handle);
   WriteStatus(status_handle, symbol, "ok", output_file, copied);
   return true;
}

void OnStart()
{
   int status_handle = FileOpen(InpStatusFile, FILE_COMMON | FILE_TXT | FILE_WRITE);
   if(status_handle == INVALID_HANDLE)
      return;

   int job_handle = FileOpen(InpJobFile, FILE_COMMON | FILE_CSV | FILE_READ, '|');
   if(job_handle == INVALID_HANDLE)
   {
      WriteStatus(status_handle, "*", "error", "job file not found", 0);
      FileClose(status_handle);
      return;
   }

   while(!FileIsEnding(job_handle))
   {
      string symbol = FileReadString(job_handle);
      if(symbol == "")
         continue;
      string timeframe_text = FileReadString(job_handle);
      string from_text = FileReadString(job_handle);
      string to_text = FileReadString(job_handle);
      string output_file = FileReadString(job_handle);

      datetime from_ts = (datetime)StringToTime(from_text);
      datetime to_ts = (datetime)StringToTime(to_text);
      ExportRates(symbol, timeframe_text, from_ts, to_ts, output_file, status_handle);
   }

   FileClose(job_handle);
   FileClose(status_handle);
}
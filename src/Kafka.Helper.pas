unit Kafka.Helper;

interface

uses
  System.SysUtils, System.Classes, System.Diagnostics, System.DateUtils, System.Generics.Collections,

  Kafka.Types,
  Kafka.Lib;

type
  EKafkaError = class(Exception);

  TOnLog = procedure(const Values: TStrings) of object;

  TOperationTimer = record
  private
    FStopwatch: TStopwatch;
    FOperation: String;
  public
    constructor Create(const Operation: String); overload;
    constructor Create(const Operation: String; Args: Array of const); overload;

    procedure Start(const Operation: String); overload;
    procedure Start(const Operation: String; Args: Array of const); overload;

    procedure Stop;

    class operator Finalize(var ADest: TOperationTimer);
  end;

  TKafkaUtils = class
  public
    class function PointerToStr(const Value: Pointer; const Len: Integer; const Encoding: TEncoding): String; static;
    class function PointerToBytes(const Value: Pointer; const Len: Integer): TBytes; static;
    class function StrToBytes(const Value: String; const Encoding: TEncoding): TBytes; static;
    class procedure StringsToConfigArrays(const Values: TStrings; out NameArr, ValueArr: TArray<String>); static;
    class function StringsToIntegerArray(const Value: String): TArray<Integer>;
    class function DateTimeToStrMS(const Value: TDateTime): String; static;
  end;

  TKafkaHelper = class
  private
    class var FLogStrings: TStringList;
    class var FOnLog: TOnLog;
    class procedure CheckKeyValues(const Keys, Values: TArray<String>); static;
  protected
    class procedure DoLog(const Text: String; const LogType: TKafkaLogType);
  public
    class constructor Create;
    class destructor Destroy;

    class procedure Log(const Text: String; const LogType: TKafkaLogType);

    // Wrappers
    class function NewConfiguration(const DefaultCallBacks: Boolean = True): prd_kafka_conf_t; overload; static;
    class function NewConfiguration(const Keys, Values: TArray<String>; const DefaultCallBacks: Boolean = True): prd_kafka_conf_t; overload; static;
    class procedure SetConfigurationValue(var Configuration: prd_kafka_conf_t; const Key, Value: String); static;
    class procedure DestroyConfiguration(const Configuration: Prd_kafka_conf_t); static;

    class function NewTopicConfiguration: prd_kafka_topic_conf_t; overload; static;
    class function NewTopicConfiguration(const Keys, Values: TArray<String>): prd_kafka_topic_conf_t; overload; static;
    class procedure SetTopicConfigurationValue(var TopicConfiguration: prd_kafka_topic_conf_t; const Key, Value: String); static;
    class procedure DestroyTopicConfiguration(const TopicConfiguration: Prd_kafka_topic_conf_t); static;

    class function NewProducer(const Configuration: prd_kafka_conf_t): prd_kafka_t; overload; static;
    class function NewProducer(const ConfigKeys, ConfigValues: TArray<String>): prd_kafka_t; overload; static;

    class function NewConsumer(const Configuration: prd_kafka_conf_t): prd_kafka_t; overload; static;

    class procedure ConsumerClose(const KafkaHandle: prd_kafka_t); static;
    class procedure DestroyHandle(const KafkaHandle: prd_kafka_t); static;

    class function NewTopic(const KafkaHandle: prd_kafka_t; const TopicName: String; const TopicConfiguration: prd_kafka_topic_conf_t = nil): prd_kafka_topic_t;
    class function NewHeaders(const Headers: TArray<TMsgHeader>): Prd_kafka_headers_t;
    class procedure DestroyHeaders(const KHeaders: Prd_kafka_headers_t);
    class function Produce(const Topic: prd_kafka_topic_t; const Payload: Pointer; const PayloadLength: NativeUInt; const Key: Pointer = nil; const KeyLen: NativeUInt = 0; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    class function Produce(const Topic: prd_kafka_topic_t; const Payloads: TArray<Pointer>; const PayloadLengths: TArray<Integer>; const Key: Pointer = nil; const KeyLen: NativeUInt = 0; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    class function Produce(const Topic: prd_kafka_topic_t; const Payload: String; const Key: String; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    class function Produce(const Topic: prd_kafka_topic_t; const Payload: String; const Key: String; const Encoding: TEncoding; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    class function Produce(const Topic: prd_kafka_topic_t; const Payloads: TArray<String>; const Key: String; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    class function Produce(const Topic: prd_kafka_topic_t; const Payloads: TArray<String>; const Key: String; const Encoding: TEncoding; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    class function Produce(const Produser: prd_kafka_t; const Topic: prd_kafka_topic_t;
                           const Payload, Key: String; const KHeaders: Prd_kafka_headers_t; const Encoding: TEncoding; const Partition: Int32; const MsgFlags: Integer;
                           const MsgOpaque: Pointer): Integer; overload;
    class procedure Flush(const KafkaHandle: prd_kafka_t; const Timeout: Integer = 1000);

    class function GetHeaders(const Msg: Prd_kafka_message_t; const Headers: TList<TMsgHeader>): TList<TMsgHeader>;
    // Utils
    class function IsKafkaError(const Error: rd_kafka_resp_err_t): Boolean; static;

    // Internal
    class procedure FlushLogs;

    class property OnLog: TOnLog read FOnLog write FOnLog;
  end;

implementation

resourcestring
  StrLogCallBackFac = 'Log_CallBack - fac = %s, buff = %s';
  StrMessageSendResult = 'Message send result = %d';
  StrErrorCallBackReaso = 'Error =  %s';
  StrUnableToCreateKaf = 'Unable to create Kafka Handle - %s';
  StrGetHeader = 'Get header error - %s';
  StrKeysAndValuesMust = 'Keys and Values must be the same length';
  StrMessageNotQueued = 'Message not Queued';
  StrInvalidConfiguratio = 'Invalid configuration key';
  StrInvalidTopicConfig = 'Invalid topic configuration key';
  StrCriticalError = 'Critical Error: ';

// Global callbacks

procedure ProducerCallBackLogger(rk: prd_kafka_t; rkmessage: prd_kafka_message_t;
  opaque: Pointer); cdecl;
begin
  if rkmessage <> nil then
  begin
    TKafkaHelper.Log(format(StrMessageSendResult, [Integer(rkmessage.err)]), TKafkaLogType.kltProducer);
  end;
end;

procedure LogCallBackLogger(rk: prd_kafka_t; level: integer; fac: PAnsiChar;
  buf: PAnsiChar); cdecl;
begin
  TKafkaHelper.Log(format(StrLogCallBackFac, [String(fac), String(buf)]), TKafkaLogType.kltLog);
end;

procedure ErrorCallBackLogger(rk: prd_kafka_t; err: integer; reason: PAnsiChar;
  opaque: Pointer); cdecl;
begin
  TKafkaHelper.Log(format(StrErrorCallBackReaso, [String(reason)]), kltError);
end;

{ TOperationTimer }

constructor TOperationTimer.Create(const Operation: String);
begin
  Start(Operation);
end;

constructor TOperationTimer.Create(const Operation: String; Args: Array of const);
begin
  Create(format(Operation, Args));
end;

procedure TOperationTimer.Start(const Operation: String);
begin
  Stop;

  FOperation := Operation;

  TKafkaHelper.Log('Started - ' + FOperation, TKafkaLogType.kltDebug);

  FStopwatch.Start;
end;

procedure TOperationTimer.Start(const Operation: String; Args: Array of const);
begin
  Start(format(Operation, Args));
end;

procedure TOperationTimer.Stop;
begin
  if FStopwatch.IsRunning then
  begin
    FStopwatch.Stop;

    TKafkaHelper.Log('Finished - ' + FOperation + ' in ' + FStopwatch.ElapsedMilliseconds.ToString + 'ms', TKafkaLogType.kltDebug);
  end;
end;

class operator TOperationTimer.Finalize(var ADest: TOperationTimer);
begin
  if ADest.FStopwatch.IsRunning then
  begin
    ADest.FStopwatch.Stop;

    TKafkaHelper.Log('Finished - ' + ADest.FOperation + ' in ' + ADest.FStopwatch.ElapsedMilliseconds.ToString + 'ms', TKafkaLogType.kltDebug);
  end;
end;

{ TKafkaUtils }

class procedure TKafkaUtils.StringsToConfigArrays(const Values: TStrings; out NameArr, ValueArr: TArray<String>);
var
  i: Integer;
  KeyValue: TArray<String>;
begin
  for i := 0 to pred(Values.Count) do
  begin
    if pos('=', Values[i]) <> 0 then
    begin
      KeyValue := Values[i].Split(['='], 2);

      NameArr := NameArr + [KeyValue[0]];
      ValueArr := ValueArr + [KeyValue[1]];
    end;
  end;
end;

class function TKafkaUtils.StringsToIntegerArray(const Value: String): TArray<Integer>;
var
  StrArray: TArray<String>;
  i: Integer;
begin
  StrArray := Value.Split([',']);

  SetLength(Result, length(StrArray));

  for i := Low(StrArray) to High(StrArray) do
  begin
    Result[i] := StrToInt(StrArray[i]);
  end;
end;

class function TKafkaUtils.DateTimeToStrMS(const Value: TDateTime): String;
begin
  Result := DateTimeToStr(Value) + '.' + MilliSecondOf(Value).ToString.PadLeft(3, '0');
end;

class function TKafkaUtils.StrToBytes(const Value: String; const Encoding: TEncoding): TBytes;
begin
  if Value = '' then
  begin
    Result := [];
  end
  else
  begin
    Result := Encoding.GetBytes(Value);
  end;
end;

class function TKafkaUtils.PointerToStr(const Value: Pointer; const Len: Integer; const Encoding: TEncoding): String;
var
  Data: TBytes;
begin
  SetLength(Data, Len);
  Move(Value^, Pointer(Data)^, Len);

  Result := Encoding.GetString(Data);
end;

class function TKafkaUtils.PointerToBytes(const Value: Pointer; const Len: Integer): TBytes;
begin
  SetLength(Result, Len);
  Move(Value^, Pointer(Result)^, Len);
end;


{ TKafkaHelper }

class procedure TKafkaHelper.ConsumerClose(const KafkaHandle: prd_kafka_t);
begin
  rd_kafka_consumer_close(KafkaHandle);
end;

class procedure TKafkaHelper.DestroyHandle(const KafkaHandle: prd_kafka_t);
begin
  rd_kafka_destroy(KafkaHandle);
end;

class constructor TKafkaHelper.Create;
begin
  FLogStrings := TStringList.Create;
end;

class destructor TKafkaHelper.Destroy;
begin
  FreeAndNil(FLogStrings);
end;

class procedure TKafkaHelper.DoLog(const Text: String; const LogType: TKafkaLogType);
begin
  TMonitor.Enter(TKafkaHelper.FLogStrings);
  try
    TKafkaHelper.FLogStrings.AddObject(Text, TObject(LogType));
  finally
    TMonitor.Exit(TKafkaHelper.FLogStrings);
  end;
end;

class procedure TKafkaHelper.Flush(const KafkaHandle: prd_kafka_t; const Timeout: Integer);
begin
  TOperationTimer.Create('Flushing');

  rd_kafka_flush(KafkaHandle, Timeout);
end;

class procedure TKafkaHelper.FlushLogs;
begin
  TMonitor.Enter(TKafkaHelper.FLogStrings);
  try
    if Assigned(FOnLog) then
    begin
      FOnLog(TKafkaHelper.FLogStrings);
    end;

    TKafkaHelper.FLogStrings.Clear;
  finally
    TMonitor.Exit(TKafkaHelper.FLogStrings);
  end;
end;
class function TKafkaHelper.GetHeaders(const Msg: Prd_kafka_message_t;const Headers: TList<TMsgHeader>): TList<TMsgHeader>;
var
  PHeaders : Prd_kafka_headers_t;
  Res : rd_kafka_resp_err_t;
  i : integer;
  cnt : integer;
  name: PAnsiChar;
  value: Pointer;
  size: PNativeUInt;
  header: TMsgHeader;
begin

  Res :=  rd_kafka_message_headers(Msg, @PHeaders);

  if (Res = RD_KAFKA_RESP_ERR_NO_ERROR) then
  begin
    cnt := rd_kafka_header_cnt(PHeaders);
    for i := 0 to cnt - 1 do
    begin
      Res := rd_kafka_header_get_all(PHeaders, i, @name, @value, @size);
      if Res = RD_KAFKA_RESP_ERR_NO_ERROR then
      begin
        header.Name :=  StrPas(name);
        header.Value := TKafkaUtils.PointerToBytes(value, Integer(size));
        Headers.Add(header);
      end else begin
        raise EKafkaError.CreateFmt(StrGetHeader, [rd_kafka_err2str(Res)])
      end;
    end;
    PHeaders := nil;
  end else begin
    //Если не равно значению нет заголовков, то ошибка
    if Res <> RD_KAFKA_RESP_ERR__NOENT then
      raise EKafkaError.CreateFmt(StrGetHeader, [rd_kafka_err2str(Res)])
  end;
  Result := Headers;
end;

class procedure TKafkaHelper.Log(const Text: String; const LogType: TKafkaLogType);
begin
  DoLog(Text, LogType);
end;

class procedure TKafkaHelper.DestroyConfiguration(const Configuration: Prd_kafka_conf_t);
begin
  rd_kafka_conf_destroy(Configuration);
end;

class procedure TKafkaHelper.DestroyTopicConfiguration(const TopicConfiguration: Prd_kafka_topic_conf_t);
begin
  rd_kafka_topic_conf_destroy(TopicConfiguration);
end;

class function TKafkaHelper.NewConfiguration(const Keys: TArray<String>; const Values: TArray<String>; const DefaultCallBacks: Boolean): prd_kafka_conf_t;
var
  i: Integer;
begin
  CheckKeyValues(Keys, Values);

  Result := rd_kafka_conf_new();

  for i := Low(keys) to High(Keys) do
  begin
    SetConfigurationValue(
      Result,
      Keys[i],
      Values[i]);
  end;

  if DefaultCallBacks then
  begin
    rd_kafka_conf_set_dr_msg_cb(Result, @ProducerCallBackLogger);
    rd_kafka_conf_set_log_cb(Result, @LogCallBackLogger);
    rd_kafka_conf_set_error_cb(Result, @ErrorCallBackLogger);
  end;
end;

class function TKafkaHelper.NewProducer(const ConfigKeys, ConfigValues: TArray<String>): prd_kafka_t;
var
  Configuration: prd_kafka_conf_t;
begin
  Configuration := TKafkaHelper.NewConfiguration(
    ConfigKeys,
    ConfigValues);

  Result := NewProducer(Configuration);
end;

class function TKafkaHelper.NewConsumer(const Configuration: prd_kafka_conf_t): prd_kafka_t;
var
  ErrorStr: TKafkaErrorArray;
begin
  Result := rd_kafka_new(
    RD_KAFKA_CONSUMER,
    Configuration,
    ErrorStr,
    Sizeof(ErrorStr));

  if Result = nil then
  begin
    raise EKafkaError.CreateFmt(StrUnableToCreateKaf, [String(ErrorStr)]);
  end;
end;

class function TKafkaHelper.NewProducer(const Configuration: prd_kafka_conf_t): prd_kafka_t;
var
  ErrorStr: TKafkaErrorArray;
begin
  Result := rd_kafka_new(
    RD_KAFKA_PRODUCER,
    Configuration,
    ErrorStr,
    Sizeof(ErrorStr));

  if Result = nil then
  begin
    raise EKafkaError.CreateFmt(StrUnableToCreateKaf, [String(ErrorStr)]);
  end;
end;

class function TKafkaHelper.NewTopic(const KafkaHandle: prd_kafka_t; const TopicName: String; const TopicConfiguration: prd_kafka_topic_conf_t): prd_kafka_topic_t;
begin
  Result := rd_kafka_topic_new(
    KafkaHandle,
    PAnsiChar(AnsiString(TopicName)),
    TopicConfiguration);

  if Result = nil then
  begin
    raise EKafkaError.Create(String(rd_kafka_err2str(rd_kafka_last_error)));
  end;
end;

class function TKafkaHelper.NewHeaders(const Headers: TArray<TMsgHeader>): Prd_kafka_headers_t;
var 
  i: Integer;
  AddResult: rd_kafka_resp_err_t;
begin
  Result := nil;
  if Length(Headers) > 0 then
  begin
    Result := rd_kafka_headers_new(Length(Headers));

    for i := 0 to Length(Headers) - 1 do
    begin
      AddResult := rd_kafka_header_add(Result, PAnsiChar(AnsiString(Headers[i].Name)), length(Headers[i].Name), @Headers[i].Value[0], length(Headers[i].Value));
      if AddResult <> RD_KAFKA_RESP_ERR_NO_ERROR then
        raise EKafkaError.CreateFmt('Error added header to message = %s', [rd_kafka_err2str(AddResult)]);
    end;
  end;
end;

class procedure TKafkaHelper.DestroyHeaders(const KHeaders: Prd_kafka_headers_t);
begin
  if KHeaders <> nil then
    rd_kafka_headers_destroy(KHeaders);
end;

class function TKafkaHelper.NewTopicConfiguration: prd_kafka_topic_conf_t;
begin
  Result := NewTopicConfiguration([], []);
end;

class procedure TKafkaHelper.CheckKeyValues(const Keys, Values: TArray<String>);
begin
  if length(keys) <> length(values) then
  begin
    raise EKafkaError.Create(StrKeysAndValuesMust);
  end;
end;

class function TKafkaHelper.NewTopicConfiguration(const Keys, Values: TArray<String>): prd_kafka_topic_conf_t;
var
  i: Integer;
begin
  Result := rd_kafka_topic_conf_new;

  CheckKeyValues(Keys, Values);

  for i := Low(keys) to High(Keys) do
  begin
    SetTopicConfigurationValue(
      Result,
      Keys[i],
      Values[i]);
  end;
end;

class function TKafkaHelper.Produce(const Produser: prd_kafka_t; const Topic: prd_kafka_topic_t;
  const Payload, Key: String; const KHeaders: Prd_kafka_headers_t;
  const Encoding: TEncoding; const Partition: Int32; const MsgFlags: Integer;
  const MsgOpaque: Pointer): Integer;
var
  KeyBytes, PayloadBytes: TBytes;
  line: array[0..6] of rd_kafka_vu_t;
  _value, _key: _anonymous_type_1;
  PRes : Prd_kafka_error_t;
  PropsCount: integer;
begin
  PropsCount := 6;

  KeyBytes := TKafkaUtils.StrToBytes(Key, Encoding);
  PayloadBytes := TKafkaUtils.StrToBytes(Payload, Encoding);

  line[0].vtype := RD_KAFKA_VTYPE_RKT;
  line[0].u.rkt := Topic;

  _value.ptr := @PayloadBytes[0];
  _value.size := length(PayloadBytes);

  line[1].vtype := RD_KAFKA_VTYPE_VALUE;
  line[1].u.mem := _value;

  _key.ptr := @KeyBytes[0];
  _key.size := length(KeyBytes);

  line[2].vtype := RD_KAFKA_VTYPE_KEY;
  line[2].u.mem := _key;

  line[3].vtype := RD_KAFKA_VTYPE_PARTITION;
  line[3].u.i32 := Partition;

  line[4].vtype := RD_KAFKA_VTYPE_MSGFLAGS;
  line[4].u.i := MsgFlags;

  line[5].vtype := RD_KAFKA_VTYPE_OPAQUE;
  line[5].u.ptr := MsgOpaque;

  if KHeaders <> nil then
  begin

    line[6].vtype := RD_KAFKA_VTYPE_HEADERS;
    line[6].u.headers := rd_kafka_headers_copy(KHeaders);

    PropsCount := PropsCount + 1;
  end;

  PRes := rd_kafka_produceva(Produser, @line[0], PropsCount);

  if PRes <> nil then
  begin
    if (KHeaders <> nil) and (line[6].u.headers <> nil) then
       rd_kafka_headers_destroy(line[6].u.headers);
    raise EKafkaError.Create(StrMessageNotQueued);
  end;

  Result := 0;
end;

class function TKafkaHelper.Produce(const Topic: prd_kafka_topic_t; const Payloads: TArray<Pointer>; const PayloadLengths: TArray<Integer>; const Key: Pointer;
  const KeyLen: NativeUInt; const Partition: Int32; const MsgFlags: Integer; const MsgOpaque: Pointer): Integer;
var
  i: Integer;
  Msgs: TArray<rd_kafka_message_t>;
  Msg: rd_kafka_message_t;
begin
  if length(Payloads) = 0 then
  begin
    Result := 0;
  end
  else
  begin
    SetLength(Msgs, length(Payloads));

    for i := Low(Payloads) to High(Payloads) do
    begin
      Msg.partition := Partition;
      Msg.rkt := Topic;
      Msg.payload := Payloads[i];
      Msg.len := PayloadLengths[i];
      Msg.key := Key;
      Msg.key_len := KeyLen;

      Msgs[i] := Msg;
    end;

    Result := rd_kafka_produce_batch(
      Topic,
      Partition,
      MsgFlags,
      @Msgs[0],
      length(Payloads));

    if Result <> length(Payloads) then
    begin
      raise EKafkaError.Create(StrMessageNotQueued);
    end;
  end;
end;

class function TKafkaHelper.Produce(const Topic: prd_kafka_topic_t; const Payloads: TArray<String>; const Key: String; const Encoding: TEncoding;
  const Partition: Int32; const MsgFlags: Integer; const MsgOpaque: Pointer): Integer;
var
  PayloadPointers: TArray<Pointer>;
  PayloadLengths: TArray<Integer>;
  KeyBytes, PayloadBytes: TBytes;
  i: Integer;
  KeyData: TBytes;
begin
  {$IFDEF DEBUG}var Timer := TOperationTimer.Create('Formatting %d messages', [Length(Payloads)]);{$ENDIF}

  SetLength(PayloadPointers, length(Payloads));
  SetLength(PayloadLengths, length(Payloads));

  KeyData := TEncoding.UTF8.GetBytes(Key);

  KeyBytes := TKafkaUtils.StrToBytes(Key, Encoding);

  for i := Low(Payloads) to High(Payloads) do
  begin
    PayloadBytes := TKafkaUtils.StrToBytes(Payloads[i], Encoding);

    PayloadPointers[i] := @PayloadBytes[0];
    PayloadLengths[i] := Length(PayloadBytes);
  end;

  {$IFDEF DEBUG}Timer.Start('Producing %d messages', [Length(Payloads)]);{$ENDIF}

  Result := Produce(
    Topic,
    PayloadPointers,
    PayloadLengths,
    @KeyBytes[0],
    Length(KeyBytes),
    Partition,
    MsgFlags,
    MsgOpaque);
end;

class function TKafkaHelper.Produce(const Topic: prd_kafka_topic_t; const Payload: String; const Key: String; const Encoding: TEncoding;
  const Partition: Int32; const MsgFlags: Integer; const MsgOpaque: Pointer): Integer;
var
  KeyBytes, PayloadBytes: TBytes;
begin
  KeyBytes := TKafkaUtils.StrToBytes(Key, TEncoding.UTF8);
  PayloadBytes := TKafkaUtils.StrToBytes(Payload, TEncoding.UTF8);

  Result := Produce(
    Topic,
    @PayloadBytes[0],
    length(PayloadBytes),
    @KeyBytes[0],
    length(KeyBytes),
    Partition,
    MsgFlags,
    MsgOpaque)
end;

class function TKafkaHelper.Produce(const Topic: prd_kafka_topic_t; const Payload: String; const Key: String; const Partition: Int32;
  const MsgFlags: Integer; const MsgOpaque: Pointer): Integer;
begin
  Result := Produce(
    Topic,
    Payload,
    Key,
    TEncoding.UTF8,
    Partition,
    MsgFlags,
    MsgOpaque);
end;

class function TKafkaHelper.Produce(const Topic: prd_kafka_topic_t; const Payload: Pointer; const PayloadLength: NativeUInt;
  const Key: Pointer; const KeyLen: NativeUInt; const Partition: Int32; const MsgFlags: Integer; const MsgOpaque: Pointer): Integer;
begin
  Result := rd_kafka_produce(
    Topic,
    Partition,
    MsgFlags,
    Payload,
    PayloadLength,
    Key,
    KeyLen,
    MsgOpaque);

  if Result = -1 then
  begin
    raise EKafkaError.Create(StrMessageNotQueued);
  end;
end;

class function TKafkaHelper.NewConfiguration(const DefaultCallBacks: Boolean): prd_kafka_conf_t;
begin
  Result := NewConfiguration([], [], DefaultCallBacks);
end;

class procedure TKafkaHelper.SetConfigurationValue(var Configuration: prd_kafka_conf_t; const Key, Value: String);
var
  ErrorStr: TKafkaErrorArray;
begin
  if Value = '' then
  begin
    raise EKafkaError.Create(StrInvalidConfiguratio);
  end;

  if rd_kafka_conf_set(
    Configuration,
    PAnsiChar(AnsiString(Key)),
    PAnsiChar(AnsiString(Value)),
    ErrorStr,
    Sizeof(ErrorStr)) <> RD_KAFKA_CONF_OK then
  begin
    raise EKafkaError.Create(String(ErrorStr));
  end;
end;

class procedure TKafkaHelper.SetTopicConfigurationValue(var TopicConfiguration: prd_kafka_topic_conf_t; const Key, Value: String);
var
  ErrorStr: TKafkaErrorArray;
begin
  if Value = '' then
  begin
    raise EKafkaError.Create(StrInvalidTopicConfig);
  end;

  if rd_kafka_topic_conf_set(
    TopicConfiguration,
    PAnsiChar(AnsiString(Key)),
    PAnsiChar(AnsiString(Value)),
    ErrorStr,
    Sizeof(ErrorStr)) <> RD_KAFKA_CONF_OK then
  begin
    raise EKafkaError.Create(String(ErrorStr));
  end;
end;

class function TKafkaHelper.IsKafkaError(const Error: rd_kafka_resp_err_t): Boolean;
begin
  Result :=
    (Error <> RD_KAFKA_RESP_ERR_NO_ERROR) and
    (Error <> RD_KAFKA_RESP_ERR__PARTITION_EOF);
end;

class function TKafkaHelper.Produce(const Topic: prd_kafka_topic_t; const Payloads: TArray<String>; const Key: String; const Partition: Int32;
   const MsgFlags: Integer; const MsgOpaque: Pointer): Integer;
begin
  Result := Produce(
    Topic,
    Payloads,
    Key,
    TEncoding.UTF8,
    Partition,
    MsgFlags,
    MsgOpaque);
end;


end.

unit Kafka.Types;

interface

uses
  System.SysUtils;

type
  PNativeUInt = ^NativeUInt;
  mode_t = LongWord;

  TKafkaErrorArray= Array[0 .. 512] of AnsiChar;

  TKafkaLogType = (
    kltLog,
    kltError,
    kltProducer,
    kltConsumer,
    kltDebug
  );

  TMsgHeader = record
    Name: string;
    Value: TBytes;
  end;

const
  KafkaLogTypeDescriptions: Array[TKafkaLogType] of String = (
   'Log',
   'Error',
   'Producer',
   'Consumer',
   'Debug'
  );

implementation

end.

unit Kafka.FMX.Form.Producer;

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Classes, System.Variants,
  System.DateUtils, System.Generics.Collections, System.Rtti,

  FMX.Types, FMX.Controls, FMX.Forms, FMX.Graphics, FMX.Dialogs, System.Actions,
  FMX.ActnList, FMX.Memo.Types, FMX.Edit, FMX.StdCtrls, FMX.ScrollBox, FMX.Memo,
  FMX.EditBox, FMX.SpinBox, FMX.Layouts, FMX.Controls.Presentation, FMX.Ani,
  FMX.Grid.Style, FMX.Grid,

  Kafka.Lib,
  Kafka.Factory,
  Kafka.Interfaces,
  Kafka.Helper,
  Kafka.Types;

type
  THeader = record
    IsUse: boolean;
    Name: string;
    Value: TBytes;
  end;

  TfrmProduce = class(TForm)
    tmrUpdate: TTimer;
    lblStatus: TLabel;
    Layout1: TLayout;
    Layout2: TLayout;
    Label2: TLabel;
    edtTopic: TEdit;
    Layout4: TLayout;
    Label3: TLabel;
    edtMessage: TEdit;
    Layout5: TLayout;
    Label4: TLabel;
    edtKey: TEdit;
    Layout3: TLayout;
    edtMessageCount: TSpinBox;
    Button1: TButton;
    Button4: TButton;
    chkFlushAfterProduce: TCheckBox;
    memConfig: TMemo;
    ActionList1: TActionList;
    Layout6: TLayout;
    Label1: TLabel;
    edtPartition: TSpinBox;
    Panel2: TPanel;
    grdHeaders: TGrid;
    Label7: TLabel;
    HeaderName: TStringColumn;
    HeaderValue: TStringColumn;
    Splitter1: TSplitter;
    FloatAnimation1: TFloatAnimation;
    HeaderUse: TCheckColumn;
    procedure ActionList1Execute(Action: TBasicAction; var Handled: Boolean);
    procedure tmrUpdateTimer(Sender: TObject);
    procedure memConfigChange(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure Action1Execute(Sender: TObject);
    procedure Button1Click(Sender: TObject);
    procedure Button4Click(Sender: TObject);
    procedure GrdSetValue(Sender: TObject; const ACol, ARow: Integer;
      const Value: TValue);
    procedure grdHeadersGetValue(Sender: TObject; const ACol, ARow: Integer;
      var Value: TValue);
  private
    FKafkaProducer: IKafkaProducer;
    FKafkaCluster: String;
    FStringEncoding: TEncoding;
    FHeaders: TList<THeader>;
    procedure UpdateStatus;
    procedure Start;
    procedure UpdateRowCount();
  public
    constructor Create(AOwner: TComponent); override;
    destructor Destroy; override;
    procedure Execute(const KafkaServers: String);
  end;

var
  frmProduce: TfrmProduce;

implementation

{$R *.fmx}

procedure TfrmProduce.Action1Execute(Sender: TObject);
var
  Msgs: TArray<String>;
  i: Integer;
begin
  Start;

  SetLength(Msgs, Trunc(edtMessageCount.Value));

  for i := 0 to pred(Trunc(edtMessageCount.Value)) do
  begin
    Msgs[i] := edtMessage.Text + ' - ' + DateTimeToStr(now) + '.' + MilliSecondOf(now).ToString.PadLeft(3, '0');
  end;

  FKafkaProducer.Produce(
    edtTopic.Text,
    Msgs,
    edtKey.Text,
    FStringEncoding,
    RD_KAFKA_PARTITION_UA,
    RD_KAFKA_MSG_F_COPY,
    @self);

  if chkFlushAfterProduce.IsChecked then
  begin
    TKafkaHelper.Flush(FKafkaProducer.KafkaHandle);
  end;
end;

procedure TfrmProduce.ActionList1Execute(Action: TBasicAction; var Handled: Boolean);
begin
  //actFlush.Enabled := FKafkaProducer <> nil;

  Handled := True;
end;

procedure TfrmProduce.Button1Click(Sender: TObject);
var
  Msgs: TArray<String>;
  Hdrs: TArray<TMsgHeader>;
  i, j: Integer;
  CntHeader: Integer;
begin
  Start;

  SetLength(Msgs, Trunc(edtMessageCount.Value));

  for i := 0 to pred(Trunc(edtMessageCount.Value)) do
  begin
    Msgs[i] := edtMessage.Text + ' - ' + DateTimeToStr(now) + '.' + MilliSecondOf(now).ToString.PadLeft(3, '0');
  end;

  CntHeader := 0;
  for i := 0 to Fheaders.Count - 1 do
    if FHeaders.Items[i].IsUse then
        CntHeader := CntHeader + 1;
  SetLength(Hdrs, CntHeader);
  try
    j := 0;
    for i := 0 to FHeaders.Count - 1 do
    begin
      if FHeaders.Items[i].IsUse then
      begin
        Hdrs[j].Name := FHeaders.Items[i].Name;
        Hdrs[j].Value := FHeaders.Items[i].Value;
        j := j + 1;
      end;
    end;
    FKafkaProducer.Produce(
      edtTopic.Text,
      Msgs[0],
      edtKey.Text,
      Hdrs,
      Trunc(edtPartition.Value),
      RD_KAFKA_MSG_F_COPY,
      @self);
  finally
    SetLength(Hdrs, 0);
  end;

  if chkFlushAfterProduce.IsChecked then
  begin
    TKafkaHelper.Flush(FKafkaProducer.KafkaHandle);
  end;
end;

procedure TfrmProduce.Button4Click(Sender: TObject);
begin
  Start;

  TKafkaHelper.Flush(FKafkaProducer.KafkaHandle);
end;

constructor TfrmProduce.Create(AOwner: TComponent);
begin
  inherited;

  FStringEncoding := TEncoding.UTF8;
  FHeaders := TList<THeader>.Create;
  UpdateRowCount();
  UpdateStatus;
end;

destructor TfrmProduce.Destroy;
begin
  if Assigned(FHeaders) then
     FreeAndNil(FHeaders);
  inherited Destroy;
end;

procedure TfrmProduce.Execute(const KafkaServers: String);
begin
  FKafkaCluster := KafkaServers;

  memConfig.Text := 'bootstrap.servers=' + KafkaServers;

  Show;

  Start;
end;

procedure TfrmProduce.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  Action := TCloseAction.caFree;
end;

procedure TfrmProduce.grdHeadersGetValue(Sender: TObject; const ACol,
  ARow: Integer; var Value: TValue);
begin
  if ARow > FHeaders.Count -1 then Exit;
  
  case ACol of
    0: Value := FHeaders[ARow].IsUse;
    1: Value := FHeaders[ARow].Name;
    2: Value := FStringEncoding.GetString(FHeaders[ARow].Value);
  end;
end;

procedure TfrmProduce.GrdSetValue(Sender: TObject; const ACol, ARow: Integer;
  const Value: TValue);
var
  header : THeader;
begin
  if ARow > FHeaders.Count - 1 then
  begin
    FHeaders.Insert(ARow, header);
  end;

  header := FHeaders.Items[ARow];

  case ACol of
    0: header.IsUse := Value.AsBoolean;
    1: header.Name := Value.AsString;
    2: header.Value := TKafkaUtils.StrToBytes(Value.AsString, TEncoding.UTF8);
  end;

  header.IsUse := (ACol > 0) and (length(header.Name) > 0);

  FHeaders.Items[ARow] := header;
  UpdateRowCount();

  if ACol = 0 then
    grdHeaders.SelectCell(1, ARow);
  if ACol = 1 then
    grdHeaders.SelectCell(0, ARow + 1);
end;

procedure TfrmProduce.UpdateRowCount();
begin
  grdHeaders.BeginUpdate;
  try
    grdHeaders.RowCount := FHeaders.Count + 1;
  finally
    grdHeaders.EndUpdate;
  end;
end;

procedure TfrmProduce.memConfigChange(Sender: TObject);
begin
  FKafkaProducer := nil;
end;

procedure TfrmProduce.Start;
var
  Names, Values: TArray<String>;
begin
  if FKafkaProducer = nil then
  begin
    TKafkaUtils.StringsToConfigArrays(memConfig.Lines, Names, Values);

    FKafkaProducer := TKafkaFactory.NewProducer(
      Names,
      Values);
  end;
end;

procedure TfrmProduce.tmrUpdateTimer(Sender: TObject);
begin
  UpdateStatus;
end;

procedure TfrmProduce.UpdateStatus;
var
  ProducedStr: String;
begin
  if FKafkaProducer = nil then
  begin
    ProducedStr := 'Idle';
  end
  else
  begin
    ProducedStr := FKafkaProducer.ProducedCount.ToString;
  end;

  lblStatus.Text := 'Produced: ' + ProducedStr;
end;

end.

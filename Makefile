ifndef TARGET
	TARGET=./bin/Debug
else
	TARGET=./bin/$(TARGET)
endif

MCS=mcs
MCSFLAGS=-debug --stacktrace -d:SAFE

RESGEN=resgen

LIBS=-lib:D:/PROGRA~1/MONO-1~1.3/lib\mono/1.0 -lib:D:/PROGRA~1/MONO-1~1.3/lib\mono/gtk-sharp

MAXDBCONSOLE_EXE=$(TARGET)/MaxDBConsole.exe
MAXDBCONSOLE_PDB=$(TARGET)/MaxDBConsole.exe
MAXDBCONSOLE_SRC=MaxDBConsole/AssemblyInfo.cs \
	MaxDBConsole/Class1.cs \
	MaxDBConsole/Tests/TestMaxDB.cs
MAXDBCONSOLE_RES=

MAXDBDATAPROVIDER_DLL=$(TARGET)/MaxDBDataProvider.dll
MAXDBDATAPROVIDER_PDB=$(TARGET)/MaxDBDataProvider.pdb
MAXDBDATAPROVIDER_SRC=MaxDBDataProvider/AssemblyInfo.cs \
	MaxDBDataProvider/MaxDBCommand.cs \
	MaxDBDataProvider/MaxDBConnection.cs \
	MaxDBDataProvider/MaxDBDataAdapter.cs \
	MaxDBDataProvider/MaxDBDataReader.cs \
	MaxDBDataProvider/MaxDBException.cs \
	MaxDBDataProvider/MaxDBMessages.cs \
	MaxDBDataProvider/MaxDBParameter.cs \
	MaxDBDataProvider/MaxDBParameterCollection.cs \
	MaxDBDataProvider/MaxDBTransaction.cs \
	MaxDBDataProvider/MaxDBType.cs \
	MaxDBDataProvider/MaxDBProtocol/MaxDBCache.cs \
	MaxDBDataProvider/MaxDBProtocol/MaxDBComm.cs \
	MaxDBDataProvider/MaxDBProtocol/MaxDBConsts.cs \
	MaxDBDataProvider/MaxDBProtocol/MaxDBDataPart.cs \
	MaxDBDataProvider/MaxDBProtocol/MaxDBGarbage.cs \
	MaxDBDataProvider/MaxDBProtocol/MaxDBInfo.cs \
	MaxDBDataProvider/MaxDBProtocol/MaxDBPacket.cs \
	MaxDBDataProvider/MaxDBProtocol/MaxDBTranslators.cs \
	MaxDBDataProvider/MaxDBProtocol/MaxDBValue.cs \
	MaxDBDataProvider/Utils/BigNumber.cs \
	MaxDBDataProvider/Utils/ByteArray.cs \
	MaxDBDataProvider/Utils/Crypt.cs \
	MaxDBDataProvider/Utils/Logger.cs \
	MaxDBDataProvider/Utils/SocketIntf.cs \
	MaxDBDataProvider/Utils/VDNNumber.cs \
	MaxDBDataProvider/Utils/SQLDBC.cs 

MAXDBDATAPROVIDER_RESX=MaxDBDataProvider/MaxDBMessages.resx
MAXDBDATAPROVIDER_RES=MaxDBDataProvider/MaxDBMessages.resources 

$(MAXDBCONSOLE_EXE): $(MAXDBCONSOLE_SRC) $(MAXDBDATAPROVIDER_DLL)
	-mkdir -p $(TARGET)
	$(MCS) $(MCSFLAGS) $(LIBS) -r:System.dll -r:System.Data.dll -r:System.Xml.dll -r:System.Windows.Forms -r:$(MAXDBDATAPROVIDER_DLL) -r:nunit.framework.dll -target:exe -out:$(MAXDBCONSOLE_EXE) $(MAXDBCONSOLE_SRC)

$(MAXDBDATAPROVIDER_DLL): $(MAXDBDATAPROVIDER_SRC) 
	-mkdir -p $(TARGET)
	$(RESGEN) $(MAXDBDATAPROVIDER_RESX) $(MAXDBDATAPROVIDER_RES)
	$(MCS) $(MCSFLAGS) $(LIBS) -r:System.dll -r:System.Data.dll -r:System.Xml.dll -target:library -out:$(MAXDBDATAPROVIDER_DLL) -res:$(MAXDBDATAPROVIDER_RES) $(MAXDBDATAPROVIDER_SRC)


# common targets

all:	$(MAXDBCONSOLE_EXE) \
	$(MAXDBDATAPROVIDER_DLL)

clean:
	-rm -f "$(MAXDBCONSOLE_EXE)" 2> /dev/null
	-rm -f "$(MAXDBCONSOLE_PDB)" 2> /dev/null
	-rm -f "$(MAXDBDATAPROVIDER_DLL)" 2> /dev/null
	-rm -f "$(MAXDBDATAPROVIDER_PDB)" 2> /dev/null


# project names as targets

MaxDBConsole: $(MAXDBCONSOLE_EXE)
MaxDBDataProvider: $(MAXDBDATAPROVIDER_DLL)


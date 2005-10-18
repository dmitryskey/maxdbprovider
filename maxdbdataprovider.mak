!if !defined (TARGET)
TARGET=.\bin\Debug
!else
TARGET=.\bin\$(TARGET)
!endif

MCS=mcs
!if !defined(RELEASE)
MCSFLAGS=-debug -unsafe --stacktrace
!endif

MAXDBCONSOLE_EXE=$(TARGET)\MaxDBConsole.exe
MAXDBCONSOLE_PDB=$(TARGET)\MaxDBConsole.exe
MAXDBCONSOLE_SRC=MaxDBConsole\AssemblyInfo.cs \
	MaxDBConsole\Class1.cs
MAXDBCONSOLE_RES=

MAXDBDATAPROVIDER_DLL=$(TARGET)\MaxDBDataProvider.dll
MAXDBDATAPROVIDER_PDB=$(TARGET)\MaxDBDataProvider.pdb
MAXDBDATAPROVIDER_SRC=MaxDBDataProvider\AssemblyInfo.cs \
	MaxDBDataProvider\MaxDBCommand.cs \
	MaxDBDataProvider\MaxDBConnection.cs \
	MaxDBDataProvider\MaxDBDataAdapter.cs \
	MaxDBDataProvider\MaxDBDataReader.cs \
	MaxDBDataProvider\MaxDBException.cs \
	MaxDBDataProvider\MaxDBParameter.cs \
	MaxDBDataProvider\MaxDBParameterCollection.cs \
	MaxDBDataProvider\MaxDBTransaction.cs \
	MaxDBDataProvider\MaxDBType.cs \
	MaxDBDataProvider\SQLDBC.cs
MAXDBDATAPROVIDER_RES=

$(MAXDBCONSOLE_EXE): $(MAXDBCONSOLE_SRC) $(MAXDBDATAPROVIDER_DLL)
	-md $(TARGET)
	$(MCS) $(MCSFLAGS) -r:System.dll -r:System.Data.dll -r:System.Xml.dll -r:$(MAXDBDATAPROVIDER_DLL) -target:exe -out:$(MAXDBCONSOLE_EXE) $(MAXDBCONSOLE_RES) $(MAXDBCONSOLE_SRC)

$(MAXDBDATAPROVIDER_DLL): $(MAXDBDATAPROVIDER_SRC) 
	-md $(TARGET)
	$(MCS) $(MCSFLAGS) -r:System.dll -r:System.Data.dll -r:System.Xml.dll -target:library -out:$(MAXDBDATAPROVIDER_DLL) $(MAXDBDATAPROVIDER_RES) $(MAXDBDATAPROVIDER_SRC)


# common targets

all:	$(MAXDBCONSOLE_EXE) \
	$(MAXDBDATAPROVIDER_DLL)

clean:
	-del "$(MAXDBCONSOLE_EXE)" 2> nul
	-del "$(MAXDBCONSOLE_PDB)" 2> nul
	-del "$(MAXDBDATAPROVIDER_DLL)" 2> nul
	-del "$(MAXDBDATAPROVIDER_PDB)" 2> nul


# project names as targets

MaxDBConsole: $(MAXDBCONSOLE_EXE)
MaxDBDataProvider: $(MAXDBDATAPROVIDER_DLL)


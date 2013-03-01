; MaxDBProvider.nsi
;
;--------------------------------

SetCompressor /solid lzma

;--------------------------------
;Include Modern UI
!include "MUI.nsh"

; Include functions and plugin
!addplugindir "." 

; The name of the installer
Name "MaxDB Provider"

; The file to write
OutFile "MaxDBProvider.exe"

; The default installation directory
InstallDir $PROGRAMFILES\MaxDBProvider

; Registry key to check for directory (so if you install again, it will 
; overwrite the old one automatically)
InstallDirRegKey HKLM "Software\MaxDBProvider" "Install_Dir"

;--------------------------------

; Pages

!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_LICENSE "gpl.txt"
!insertmacro MUI_PAGE_COMPONENTS
Page Custom CustomParameters
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_WELCOME  
!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_UNPAGE_FINISH

;--------------------------------
;Interface Settings

!define MUI_ABORTWARNING

Var INST_GAC_2_0
Var INST_GAC_4_0
  
;--------------------------------
;Languages
 
!insertmacro MUI_LANGUAGE "English"

;--------------------------------
;Reserve Files
  
;These files should be inserted before other files in the data block
;Keep these lines before any File command
;Only for solid compression (by default, solid compression is enabled for BZIP2 and LZMA)
  
ReserveFile "params.ini"
!insertmacro MUI_RESERVEFILE_INSTALLOPTIONS

;--------------------------------

; The stuff to install
Section "Binaries for .Net" SecNet

  SectionIn RO

  !insertmacro MUI_INSTALLOPTIONS_READ $INST_GAC_2_0 "params.ini" "Field 2" "State"
  !insertmacro MUI_INSTALLOPTIONS_READ $INST_GAC_4_0 "params.ini" "Field 3" "State"
  
  ; Set output path to the installation directory.
  SetOutPath $INSTDIR
  
  ; Put file there
  CreateDirectory "$OUTDIR\bin\net-2.0"
  CreateDirectory "$OUTDIR\bin\net-4.0"

  File "/oname=$OUTDIR\bin\net-2.0\MaxDB.Data.dll" "..\bin\net-2.0\release\MaxDB.Data.dll"
  File "/oname=$OUTDIR\bin\net-4.0\MaxDB.Data.dll" "..\bin\net-4.0\release\MaxDB.Data.dll"

  StrCmp $INST_GAC_2_0 0 gac40

  MaxDBProvider::SetWindowCursor "WAIT"
  File "/oname=$TEMP\gacutil.exe" "..\GAC\v2.0\gacutil.exe"
  MaxDBProvider::ExecWaitMin '"$TEMP\gacutil.exe" -i "$OUTDIR\bin\net-2.0\MaxDB.Data.dll"' "$TEMP"
  MaxDBProvider::SetWindowCursor "ARROW"

gac40:

  StrCmp $INST_GAC_4_0 0 reg

  MaxDBProvider::SetWindowCursor "WAIT"
  File "/oname=$TEMP\gacutil.exe" "..\GAC\v4.0\gacutil.exe"
  MaxDBProvider::ExecWaitMin '"$TEMP\gacutil.exe" -i "$OUTDIR\bin\net-4.0\MaxDB.Data.dll"' "$TEMP"
  MaxDBProvider::SetWindowCursor "ARROW"

reg:
  ; Write the installation path into the registry
  WriteRegStr HKLM SOFTWARE\MaxDBProvider "Install_Dir" "$INSTDIR"
  
  ; Write the uninstall keys for Windows
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\MaxDBProvider" "DisplayName" "ADO.NET Provider for MaxDB"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\MaxDBProvider" "UninstallString" '"$INSTDIR\uninstall.exe"'
  WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\MaxDBProvider" "NoModify" 1
  WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\MaxDBProvider" "NoRepair" 1
  WriteUninstaller "uninstall.exe"
  
SectionEnd

; Optional section (can be disabled by the user)
Section "Source Code" SecSource

  CreateDirectory "$OUTDIR\Sources"

  StrCpy $0 $OUTDIR
  SetOutPath $OUTDIR\Sources

  File /r /x bin /x obj /x _svn /x .svn /x *ReSharper* /x *StyleCop* /x *.suo /x *.ncb /x results /x *.exe /x MaxDBProvider.dll "..\*.*"

  StrCpy $OUTDIR $0

SectionEnd

; Optional section (can be disabled by the user)
Section "Start Menu Shortcuts" SecShortcuts

  CreateDirectory "$SMPROGRAMS\MaxDBProvider"
  CreateShortCut "$SMPROGRAMS\MaxDBProvider\Uninstall.lnk" "$INSTDIR\uninstall.exe" "" "$INSTDIR\uninstall.exe" 0
  
SectionEnd

;--------------------------------
;Descriptions

!insertmacro MUI_FUNCTION_DESCRIPTION_BEGIN
  !insertmacro MUI_DESCRIPTION_TEXT ${SecNet} "ADO.NET Provider for MaxDB (assemblies for .Net Framework)"
  !insertmacro MUI_DESCRIPTION_TEXT ${SecSource} "Source code of the ADO.NET provider for MaxDB"
  !insertmacro MUI_DESCRIPTION_TEXT ${SecShortcuts} "Create shortcuts to ADO.NET provider for MaxDB"
!insertmacro MUI_FUNCTION_DESCRIPTION_END

;--------------------------------
;Installer Functions

Function .onInit

   Call IsAnyDotNetInstalled
   Pop $0
   StrCmp $0 1 found.NETFramework

   MessageBox MB_ICONEXCLAMATION ".Net Framework is not found"
   abort

found.NETFramework:

  ;Extract InstallOptions INI files
  !insertmacro MUI_INSTALLOPTIONS_EXTRACT "params.ini"
  
FunctionEnd

LangString TEXT_IO_TITLE ${LANG_ENGLISH} "Parameters"
LangString TEXT_IO_SUBTITLE ${LANG_ENGLISH} "Please select install options."

Function CustomParameters

  !insertmacro MUI_HEADER_TEXT "$(TEXT_IO_TITLE)" "$(TEXT_IO_SUBTITLE)"
  !insertmacro MUI_INSTALLOPTIONS_DISPLAY "params.ini"

FunctionEnd

; IsDotAnyNetInstalled
;
; Based on GetDotNEVersion
;   http://nsis.sourceforge.net/Get_.NET_Version
;
; Usage:
;   Call IsAnyDotNetInstalled
;   Pop $0
;   StrCmp $0 1 found.NETFramework no.NETFramework

Function IsAnyDotNetInstalled
  Push $0
  Push $1

  StrCpy $0 1
  System::Call "mscoree::GetCORVersion(w, i ${NSIS_MAX_STRLEN}, *i) i .r1"
  StrCmp $1 0 +2
  StrCpy $0 0

  Pop $1
  Exch $0
FunctionEnd

;--------------------------------

; Uninstaller

Section "Uninstall"

  MaxDBProvider::SetWindowCursor "WAIT"
  MaxDBProvider::ExecWaitMin '"$TEMP\gacutil.exe" -u "MaxDB.Data"' "$TEMP"
  MaxDBProvider::SetWindowCursor "ARROW"

  ; Remove registry keys
  DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\MaxDBProvider"
  DeleteRegKey HKLM SOFTWARE\MaxDBProvider


  RMDir /r $INSTDIR
  RMDir /r $SMPROGRAMS\MaxDBProvider

SectionEnd

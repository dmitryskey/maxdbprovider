; MaxDBProvider.nsi
;
;--------------------------------

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

Var DOT_MAJOR
Var DOT_MINOR
Var INST_GAC
Var INST_SAFE
Var GAC_PATH_1_1
Var GAC_PATH_2_0
  
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

  !insertmacro MUI_INSTALLOPTIONS_READ $INST_GAC "params.ini" "Field 2" "State"
  !insertmacro MUI_INSTALLOPTIONS_READ $INST_SAFE "params.ini" "Field 3" "State"
  
  ; Set output path to the installation directory.
  SetOutPath $INSTDIR
  
  ; Put file there
  CreateDirectory "$OUTDIR\bin\net-1.1"
  CreateDirectory "$OUTDIR\bin\net-2.0"

  StrCmp $INST_SAFE 0 unsafe
  File "/oname=$OUTDIR\bin\net-1.1\MaxDB.Data.dll" "..\bin\net-1.1\safe\release\MaxDB.Data.dll"
  File "/oname=$OUTDIR\bin\net-1.1\org.mentalis.security.dll" "..\bin\net-1.1\safe\release\org.mentalis.security.dll"
  File "/oname=$OUTDIR\bin\net-2.0\MaxDB.Data.dll" "..\bin\net-2.0\safe\release\MaxDB.Data.dll"

  Goto gac

unsafe:
  File "/oname=$OUTDIR\bin\net-1.1\MaxDB.Data.dll" "..\bin\net-1.1\unsafe\release\MaxDB.Data.dll"
  File "/oname=$OUTDIR\bin\net-2.0\MaxDB.Data.dll" "..\bin\net-2.0\unsafe\release\MaxDB.Data.dll"

gac:

  StrCmp $INST_GAC 0 reg

  MaxDBProvider::SetWindowCursor "WAIT"
  MaxDBProvider::ExecWaitMin '"$GAC_PATH_1_1" -i "$OUTDIR\bin\net-1.1\MaxDB.Data.dll"' "$TEMP"
  MaxDBProvider::ExecWaitMin '"$GAC_PATH_2_0" -i "$OUTDIR\bin\net-2.0\MaxDB.Data.dll"' "$TEMP"
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
Section "Binaries for Mono" SecMono

  CreateDirectory "$OUTDIR\bin\mono-1.0"
  CreateDirectory "$OUTDIR\bin\mono-2.0"

  StrCmp $INST_SAFE 0 unsafe
  File "/oname=$OUTDIR\bin\mono-1.0\MaxDB.Data.dll" "..\bin\mono-1.0\safe\release\MaxDB.Data.dll"
  File "/oname=$OUTDIR\bin\mono-2.0\MaxDB.Data.dll" "..\bin\mono-2.0\safe\release\MaxDB.Data.dll"
  Goto done

unsafe:
  File "/oname=$OUTDIR\bin\mono-1.0\MaxDB.Data.dll" "..\bin\mono-1.0\unsafe\release\MaxDB.Data.dll"
  File "/oname=$OUTDIR\bin\mono-2.0\MaxDB.Data.dll" "..\bin\mono-2.0\unsafe\release\MaxDB.Data.dll"

done:

SectionEnd

; Optional section (can be disabled by the user)
Section "Source Code" SecSource

  CreateDirectory "$OUTDIR\Sources"

  StrCpy $0 $OUTDIR
  SetOutPath $OUTDIR\Sources

  File /r /x bin /x obj /x _svn /x results /x *.exe "..\*.*"

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
  !insertmacro MUI_DESCRIPTION_TEXT ${SecMono} "ADO.NET Provider for MaxDB (assemblies for Mono)"
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

  StrCpy $DOT_MAJOR "1"
  StrCpy $DOT_MINOR "1"
  Call IsDotNetInstalled
  Pop $0

  StrCmp $0 0 net20

  StrCpy $GAC_PATH_1_1 "$WINDIR\Microsoft.NET\Framework\v1.1.$0\gacutil.exe"

net20:

  StrCpy $DOT_MAJOR "2"
  StrCpy $DOT_MINOR "0"
  Call IsDotNetInstalled
  Pop $0

  StrCmp $0 0 done

  StrCpy $GAC_PATH_2_0 "$PROGRAMFILES\Microsoft Visual Studio 8\SDK\v2.0\Bin\gacutil.exe"

done:
  ;Extract InstallOptions INI files
  !insertmacro MUI_INSTALLOPTIONS_EXTRACT "params.ini"
  
FunctionEnd

Function un.onInit

  StrCpy $DOT_MAJOR "1"
  StrCpy $DOT_MINOR "1"
  Call un.IsDotNetInstalled
  Pop $0

  StrCmp $0 0 net20

  StrCpy $GAC_PATH_1_1 "$WINDIR\Microsoft.NET\Framework\v1.1.$0\gacutil.exe"

net20:

  StrCpy $DOT_MAJOR "2"
  StrCpy $DOT_MINOR "0"
  Call un.IsDotNetInstalled
  Pop $0

  StrCmp $0 0 done

  StrCpy $GAC_PATH_2_0 "$PROGRAMFILES\Microsoft Visual Studio 8\SDK\v2.0\Bin\gacutil.exe"

done:
  
FunctionEnd

LangString TEXT_IO_TITLE ${LANG_ENGLISH} "Parameters"
LangString TEXT_IO_SUBTITLE ${LANG_ENGLISH} "Please select install options."

Function CustomParameters

  !insertmacro MUI_HEADER_TEXT "$(TEXT_IO_TITLE)" "$(TEXT_IO_SUBTITLE)"
  !insertmacro MUI_INSTALLOPTIONS_DISPLAY "params.ini"

FunctionEnd

; Usage
; Define in your script two constants:
;   DOT_MAJOR "(Major framework version)"
;   DOT_MINOR "{Minor frameword version)"
; 
; Call IsDotNetInstalled
; This function will abort the installation if the required version 
; or higher version of the .NETFramework is not installed.  Place it in
; either your .onInit function or your first install section before 
; other code.
Function IsDotNetInstalled
 
  StrCpy $0 "0"
  StrCpy $1 "SOFTWARE\Microsoft\.NETFramework" ;registry entry to look in.
  StrCpy $2 0

  Push $4	
 
  StartEnum:
    ;Enumerate the versions installed.
    EnumRegKey $3 HKLM "$1\policy" $2
    
    ;If we don't find any versions installed, it's not here.
    StrCmp $3 "" noDotNet notEmpty
    
    ;We found something.
    notEmpty:
      ;Find out if the RegKey starts with 'v'.  
      ;If it doesn't, goto the next key.
      StrCpy $4 $3 1 0
      StrCmp $4 "v" +1 goNext
      StrCpy $4 $3 1 1
      
      ;It starts with 'v'.  Now check to see how the installed major version
      ;relates to our required major version.
      ;If it's equal check the minor version, if it's greater, 
      ;we found a good RegKey.
      IntCmp $4 $DOT_MAJOR +1 goNext yesDotNetReg
      ;Check the minor version.  If it's equal or greater to our requested 
      ;version then we're good.
      StrCpy $4 $3 1 3
      IntCmp $4 $DOT_MINOR yesDotNetReg goNext yesDotNetReg
 
    goNext:
      ;Go to the next RegKey.
      IntOp $2 $2 + 1
      goto StartEnum
 
  yesDotNetReg:
    ;Now that we've found a good RegKey, let's make sure it's actually
    ;installed by getting the install path and checking to see if the 
    ;mscorlib.dll exists.
    EnumRegValue $2 HKLM "$1\policy\$3" 0
    ;$2 should equal whatever comes after the major and minor versions 
    ;(ie, v1.1.4322)
    StrCmp $2 "" noDotNet
    ReadRegStr $4 HKLM $1 "InstallRoot"
    ;Hopefully the install root isn't empty.
    StrCmp $4 "" noDotNet
    ;build the actuall directory path to mscorlib.dll.
    StrCpy $4 "$4$3.$2\mscorlib.dll"
    IfFileExists $4 yesDotNet noDotNet
 
  noDotNet:
    ;Nope, something went wrong along the way.  Looks like the 
    ;proper .NETFramework isn't installed.  
    StrCpy $4 0
    Goto done
	
  yesDotNet:
    ;Everything checks out.  Go on with the rest of the installation.
    StrCpy $4 $2
  done:
    Exch $4   
FunctionEnd

; Usage
; Define in your script two constants:
;   DOT_MAJOR "(Major framework version)"
;   DOT_MINOR "{Minor frameword version)"
; 
; Call IsDotNetInstalled
; This function will abort the installation if the required version 
; or higher version of the .NETFramework is not installed.  Place it in
; either your .onInit function or your first install section before 
; other code.
Function un.IsDotNetInstalled
 
  StrCpy $0 "0"
  StrCpy $1 "SOFTWARE\Microsoft\.NETFramework" ;registry entry to look in.
  StrCpy $2 0

  Push $4	
 
  StartEnum:
    ;Enumerate the versions installed.
    EnumRegKey $3 HKLM "$1\policy" $2
    
    ;If we don't find any versions installed, it's not here.
    StrCmp $3 "" noDotNet notEmpty
    
    ;We found something.
    notEmpty:
      ;Find out if the RegKey starts with 'v'.  
      ;If it doesn't, goto the next key.
      StrCpy $4 $3 1 0
      StrCmp $4 "v" +1 goNext
      StrCpy $4 $3 1 1
      
      ;It starts with 'v'.  Now check to see how the installed major version
      ;relates to our required major version.
      ;If it's equal check the minor version, if it's greater, 
      ;we found a good RegKey.
      IntCmp $4 $DOT_MAJOR +1 goNext yesDotNetReg
      ;Check the minor version.  If it's equal or greater to our requested 
      ;version then we're good.
      StrCpy $4 $3 1 3
      IntCmp $4 $DOT_MINOR yesDotNetReg goNext yesDotNetReg
 
    goNext:
      ;Go to the next RegKey.
      IntOp $2 $2 + 1
      goto StartEnum
 
  yesDotNetReg:
    ;Now that we've found a good RegKey, let's make sure it's actually
    ;installed by getting the install path and checking to see if the 
    ;mscorlib.dll exists.
    EnumRegValue $2 HKLM "$1\policy\$3" 0
    ;$2 should equal whatever comes after the major and minor versions 
    ;(ie, v1.1.4322)
    StrCmp $2 "" noDotNet
    ReadRegStr $4 HKLM $1 "InstallRoot"
    ;Hopefully the install root isn't empty.
    StrCmp $4 "" noDotNet
    ;build the actuall directory path to mscorlib.dll.
    StrCpy $4 "$4$3.$2\mscorlib.dll"
    IfFileExists $4 yesDotNet noDotNet
 
  noDotNet:
    ;Nope, something went wrong along the way.  Looks like the 
    ;proper .NETFramework isn't installed.  
    StrCpy $4 0
    Goto done
	
  yesDotNet:
    ;Everything checks out.  Go on with the rest of the installation.
    StrCpy $4 $2
  done:
    Exch $4   
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

  StrCmp $INST_GAC 0 unreg

  MaxDBProvider::SetWindowCursor "WAIT"
  MaxDBProvider::ExecWaitMin '"$GAC_PATH_1_1" -u "MaxDB.Data"' "$TEMP"
  MaxDBProvider::ExecWaitMin '"$GAC_PATH_2_0" -u "MaxDB.Data"' "$TEMP"
  MaxDBProvider::SetWindowCursor "ARROW"

unreg:
  
  ; Remove registry keys
  DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\MaxDBProvider"
  DeleteRegKey HKLM SOFTWARE\MaxDBProvider


  RMDir /r $INSTDIR
  RMDir /r $SMPROGRAMS\MaxDBProvider


SectionEnd

#define WIN32_LEAN_AND_MEAN		// Exclude rarely-used stuff from Windows headers
// Windows Header Files:
#include <windows.h>
#include <wininet.h>

#define BUF_SIZE 256

#define MAXDBPROVIDER_INIT()           {  \
        g_stringsize = string_size; \
        g_stacktop = stacktop;      \
        g_variables = variables; }

struct stack_t {
	stack_t *next;
	TCHAR text[1]; //this should be the length of string_size
};

unsigned int g_stringsize;
stack_t **g_stacktop;
TCHAR *g_variables;

int popstring(TCHAR *, int); //extract string from the stack - 0 on success, 1 on empty stack
void pushstring(TCHAR *); //put string into the stack
void ShowErrorMessage(DWORD);

//set current cursor shape - ARROW or WAIT
extern "C" __declspec(dllexport) void SetWindowCursor(HWND hwndParent, int string_size, TCHAR *variables, stack_t **stacktop)
{
	TCHAR cursor_name[BUF_SIZE]; //cursor name

	//init parameters
	MAXDBPROVIDER_INIT();

	//extract parameters
	popstring(cursor_name, BUF_SIZE);

	if (lstrcmp(TEXT("WAIT"), cursor_name) == 0)
		SetCursor(LoadCursor(0, IDC_WAIT));
	else
		if (lstrcmp(TEXT("ARROW"), cursor_name) == 0) 
			SetCursor(LoadCursor(0, IDC_ARROW));
}

//execute file in the minimized window and wait
extern "C" void __declspec(dllexport) ExecWaitMin(HWND hwndParent, int string_size, TCHAR *variables, stack_t **stacktop)
{
	TCHAR cmd[MAX_PATH]; //command line
	TCHAR dir[MAX_PATH]; //directory where to execute
	TCHAR result[BUF_SIZE]; //string buffer for the exit code

	HANDLE hProc; //process handle
	DWORD lExitCode; //exit code

	PROCESS_INFORMATION pInfo; //process information
	static STARTUPINFO start_up; //info how to laungh process

	//init parameters
	MAXDBPROVIDER_INIT();

	//extract parameters
	popstring(cmd, MAX_PATH);
	popstring(dir, MAX_PATH);
	
	//show in the minimized window - parent window is active
	start_up.cb = sizeof(STARTUPINFO);
	start_up.dwFlags = STARTF_USESHOWWINDOW;
	start_up.wShowWindow = SW_SHOWMINNOACTIVE;

	//create new process
	if (CreateProcess(NULL, cmd, NULL, NULL, FALSE, 0, NULL, dir, &start_up, &pInfo) == FALSE)
	{
		ShowErrorMessage(GetLastError());
		return;
	}
	CloseHandle(pInfo.hThread);
	hProc = pInfo.hProcess;

	//wait until the new process is finished
	if (hProc != NULL)
	{
		while (WaitForSingleObject(hProc, 100) == WAIT_TIMEOUT)
		{
			MSG msg; //ignore incoming events
			while (PeekMessage(&msg, NULL, WM_PAINT, WM_PAINT, PM_REMOVE))
				DispatchMessage(&msg);
		}
		GetExitCodeProcess(hProc, &lExitCode);

		CloseHandle( hProc );
	}

	wsprintf(result, TEXT("%d") , lExitCode);
	pushstring(result);
}

//get string from the stack
int popstring(TCHAR *str, int buffer_size)
{
	stack_t *th;
	if (!g_stacktop || !*g_stacktop) 
		return 1;
	th = *g_stacktop;
	lstrcpyn(str, th->text, buffer_size);
	*g_stacktop = th->next;
	GlobalFree((HGLOBAL)th);
	return 0;
}

//put string to the stack
void pushstring(TCHAR *str)
{
	stack_t *th;
	if (!g_stacktop) 
		return;
	th = reinterpret_cast<stack_t*>(GlobalAlloc(GPTR, sizeof(stack_t) + g_stringsize));
	lstrcpyn(th->text, str, g_stringsize);
	th->next = *g_stacktop;
	*g_stacktop = th;
}

//decode system and internet error
void ShowErrorMessage(DWORD dwError)
{
	LPVOID lpMsgBuf = 0;
	//we need special procedure for internet error
	if (dwError >= INTERNET_ERROR_BASE && dwError <= INTERNET_ERROR_LAST)
	{
		//extract additional information for extended error
		if (dwError == ERROR_INTERNET_EXTENDED_ERROR)
		{
			DWORD dwInetError;
			DWORD dwExtLength;
			if (! InternetGetLastResponseInfo(&dwInetError, 0, &dwExtLength) && GetLastError() == ERROR_INSUFFICIENT_BUFFER)
			{
				dwExtLength += 1;
				lpMsgBuf = LocalAlloc(LMEM_FIXED, dwExtLength);
				if ((lpMsgBuf != NULL) && 
					(!InternetGetLastResponseInfo(&dwInetError, reinterpret_cast<LPTSTR>(lpMsgBuf), &dwExtLength)))
				{
					LocalFree (lpMsgBuf);
					lpMsgBuf = 0;
				}
			}
		}
		else //decode WININET error
			FormatMessage (FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_IGNORE_INSERTS | FORMAT_MESSAGE_FROM_HMODULE,
				GetModuleHandle(TEXT("wininet.dll")), dwError, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), 
				reinterpret_cast<LPTSTR>(&lpMsgBuf), 0, 0);
	}
	else //decode system error
		FormatMessage (FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_IGNORE_INSERTS | FORMAT_MESSAGE_FROM_SYSTEM, 0, dwError, 
			MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), reinterpret_cast<LPTSTR>(&lpMsgBuf), 0, 0);

	if (lpMsgBuf == NULL)
		pushstring(TEXT("Неизвестная ошибка."));
	else
	{
		pushstring(reinterpret_cast<LPTSTR>(lpMsgBuf));
		LocalFree(lpMsgBuf);
	}
}

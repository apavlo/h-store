/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a preliminary
 * version of a benchmark specification being developed by the TPC. The
 * Work is being made available to the public for review and comment only.
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Sergey Vasilevskiy
 */

#ifndef ERROR_H
#define ERROR_H

#ifndef _INC_STRING
    #include <string.h>
#endif
#include <string>

namespace TPCE
{

#define ERR_TYPE_LOGIC                          -1      //logic error in program; internal error
#define ERR_SUCCESS                             0       //success (a non-error error)
#define ERR_TYPE_OS                             11      //operating system error
#define ERR_TYPE_MEMORY                         12      //memory allocation error
#define ERR_TYPE_FIXED_MAP                      27      //Error from CFixedMap
#define ERR_TYPE_FIXED_ARRAY                    28      //Error from CFixedArray
#define ERR_TYPE_CHECK                          29      //Check assertion error (from DriverParamSettings)

#define ERR_INS_MEMORY          "Insufficient Memory to continue."
#define ERR_UNKNOWN             "Unknown error."
#define ERR_MSG_BUF_SIZE        512
#define INV_ERROR_CODE          -1

class CBaseErr : public std::exception
{
protected:
        std::string m_location;
        int     m_idMsg;

public:
    CBaseErr()
        : m_location()
        , m_idMsg(INV_ERROR_CODE)
    {
    }

    CBaseErr(char const * szLoc)
        : m_location(szLoc)
        , m_idMsg(INV_ERROR_CODE)
    {
    }

    CBaseErr(int idMsg)
        : m_location()
        , m_idMsg(idMsg)
    {
    }

    CBaseErr(int idMsg, char const * szLoc)
        : m_location(szLoc)
        , m_idMsg(idMsg)
    {
    }

    ~CBaseErr() throw()
    {
    }

    virtual const char* what() const throw() {
        return ErrorText();
    }

    virtual int ErrorNum() { return m_idMsg; }
    virtual int ErrorType() = 0;    // a value which distinguishes the kind of error that occurred

    virtual const char *ErrorText() const = 0;    // a string (i.e., human readable) representation of the error
    virtual const char *ErrorLoc() { return m_location.c_str(); }
};

class CMemoryErr : public CBaseErr
{
public:
    CMemoryErr()
        : CBaseErr()
    {
    }

    CMemoryErr(char const * szLoc)
        : CBaseErr(szLoc)
    {
    }

    int ErrorType() {return ERR_TYPE_MEMORY;}
    const char *ErrorText() const {return ERR_INS_MEMORY;}
};

class CSystemErr : public CBaseErr
{
public:
    enum Action
    {
        eNone = 0,
        eTransactNamedPipe,
        eWaitNamedPipe,
        eSetNamedPipeHandleState,
        eCreateFile,
        eCreateProcess,
        eCallNamedPipe,
        eCreateEvent,
        eCreateThread,
        eVirtualAlloc,
        eReadFile = 10,
        eWriteFile,
        eMapViewOfFile,
        eCreateFileMapping,
        eInitializeSecurityDescriptor,
        eSetSecurityDescriptorDacl,
        eCreateNamedPipe,
        eConnectNamedPipe,
        eWaitForSingleObject,
        eRegOpenKeyEx,
        eRegQueryValueEx = 20,
        ebeginthread,
        eRegEnumValue,
        eRegSetValueEx,
        eRegCreateKeyEx,
        eWaitForMultipleObjects,
        eRegisterClassEx,
        eCreateWindow,
        eCreateSemaphore,
        eReleaseSemaphore,
        eFSeek,
        eFRead,
        eFWrite,
        eTmpFile,
        eSetFilePointer,
        eNew,
        eCloseHandle,
        eCreateMutex,
        eReleaseMutex
    };

            CSystemErr(Action eAction, char const * szLocation);
            CSystemErr(int iError, Action eAction, char const * szLocation);
    int     ErrorType() { return ERR_TYPE_OS;};
    const char  *ErrorText(void) const;

    Action  m_eAction;

    ~CSystemErr() throw()
    {
    }

};

class CBaseTxnErr
{
public:
    enum
    {
        SUCCESS = 0,
        //Trade Order errors
        UNAUTHORIZED_EXECUTOR,
        ROLLBACK,   // return from TradeOrderFrame5 to indicate transaction rollback
        BAD_INPUT_DATA,
        Last
    }   mErrCode;

    static const char* szMsgs[CBaseTxnErr::Last+1];
    static const char* ErrorText(int code);
};

class CCheckErr : public CBaseErr
{
private:
    std::string name_;
    std::string msg_;
public:
    CCheckErr()
        : CBaseErr()
    {
    }

    ~CCheckErr() throw()
    {
    }

    CCheckErr(const char *name, const std::string& msg)
        : CBaseErr(name)
        , msg_(msg)
    {
    }

    int ErrorType() {return ERR_TYPE_CHECK;}
    const char *ErrorText() const {
        return msg_.c_str();
    }
};

}   // namespace TPCE

#endif //ERROR_H

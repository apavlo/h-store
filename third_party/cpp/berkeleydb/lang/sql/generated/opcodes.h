/* Automatically generated.  Do not edit */
/* See the mkopcodeh.awk script for details */
#define OP_Function        1 /* synopsis: r[P3]=func(r[P2@P5])             */
#define OP_Savepoint       2
#define OP_AutoCommit      3
#define OP_Transaction     4
#define OP_SorterNext      5
#define OP_PrevIfOpen      6
#define OP_NextIfOpen      7
#define OP_Prev            8
#define OP_Next            9
#define OP_AggStep        10 /* synopsis: accum=r[P3] step(r[P2@P5])       */
#define OP_Checkpoint     11
#define OP_JournalMode    12
#define OP_Vacuum         13
#define OP_VFilter        14 /* synopsis: iPlan=r[P3] zPlan='P4'           */
#define OP_VUpdate        15 /* synopsis: data=r[P3@P2]                    */
#define OP_Goto           16
#define OP_Gosub          17
#define OP_Return         18
#define OP_Not            19 /* same as TK_NOT, synopsis: r[P2]= !r[P1]    */
#define OP_Yield          20
#define OP_HaltIfNull     21 /* synopsis: if r[P3] null then halt          */
#define OP_Halt           22
#define OP_Integer        23 /* synopsis: r[P2]=P1                         */
#define OP_Int64          24 /* synopsis: r[P2]=P4                         */
#define OP_String         25 /* synopsis: r[P2]='P4' (len=P1)              */
#define OP_Null           26 /* synopsis: r[P2..P3]=NULL                   */
#define OP_Blob           27 /* synopsis: r[P2]=P4 (len=P1)                */
#define OP_Variable       28 /* synopsis: r[P2]=parameter(P1,P4)           */
#define OP_Move           29 /* synopsis: r[P2@P3]=r[P1@P3]                */
#define OP_Copy           30 /* synopsis: r[P2@P3+1]=r[P1@P3+1]            */
#define OP_SCopy          31 /* synopsis: r[P2]=r[P1]                      */
#define OP_ResultRow      32 /* synopsis: output=r[P1@P2]                  */
#define OP_CollSeq        33
#define OP_AddImm         34 /* synopsis: r[P1]=r[P1]+P2                   */
#define OP_MustBeInt      35
#define OP_RealAffinity   36
#define OP_Permutation    37
#define OP_Compare        38
#define OP_Jump           39
#define OP_Once           40
#define OP_If             41
#define OP_IfNot          42
#define OP_Column         43 /* synopsis: r[P3]=PX                         */
#define OP_Affinity       44 /* synopsis: affinity(r[P1@P2])               */
#define OP_MakeRecord     45 /* synopsis: r[P3]=mkrec(r[P1@P2])            */
#define OP_Count          46 /* synopsis: r[P2]=count()                    */
#define OP_ReadCookie     47
#define OP_SetCookie      48
#define OP_VerifyCookie   49
#define OP_OpenRead       50 /* synopsis: root=P2 iDb=P3                   */
#define OP_OpenWrite      51 /* synopsis: root=P2 iDb=P3                   */
#define OP_OpenAutoindex  52 /* synopsis: nColumn=P2                       */
#define OP_OpenEphemeral  53 /* synopsis: nColumn=P2                       */
#define OP_SorterOpen     54
#define OP_OpenPseudo     55 /* synopsis: content in r[P2@P3]              */
#define OP_Close          56
#define OP_SeekLt         57 /* synopsis: key=r[P3@P4]                     */
#define OP_SeekLe         58 /* synopsis: key=r[P3@P4]                     */
#define OP_SeekGe         59 /* synopsis: key=r[P3@P4]                     */
#define OP_SeekGt         60 /* synopsis: key=r[P3@P4]                     */
#define OP_Seek           61 /* synopsis: intkey=r[P2]                     */
#define OP_NoConflict     62 /* synopsis: key=r[P3@P4]                     */
#define OP_NotFound       63 /* synopsis: key=r[P3@P4]                     */
#define OP_Found          64 /* synopsis: key=r[P3@P4]                     */
#define OP_NotExists      65 /* synopsis: intkey=r[P3]                     */
#define OP_Sequence       66 /* synopsis: r[P2]=rowid                      */
#define OP_NewRowid       67 /* synopsis: r[P2]=rowid                      */
#define OP_Insert         68 /* synopsis: intkey=r[P3] data=r[P2]          */
#define OP_InsertInt      69 /* synopsis: intkey=P3 data=r[P2]             */
#define OP_Delete         70
#define OP_Or             71 /* same as TK_OR, synopsis: r[P3]=(r[P1] || r[P2]) */
#define OP_And            72 /* same as TK_AND, synopsis: r[P3]=(r[P1] && r[P2]) */
#define OP_ResetCount     73
#define OP_SorterCompare  74 /* synopsis: if key(P1)!=rtrim(r[P3],P4) goto P2 */
#define OP_SorterData     75 /* synopsis: r[P2]=data                       */
#define OP_IsNull         76 /* same as TK_ISNULL, synopsis: if r[P1]==NULL goto P2 */
#define OP_NotNull        77 /* same as TK_NOTNULL, synopsis: if r[P1]!=NULL goto P2 */
#define OP_Ne             78 /* same as TK_NE, synopsis: if r[P1]!=r[P3] goto P2 */
#define OP_Eq             79 /* same as TK_EQ, synopsis: if r[P1]==r[P3] goto P2 */
#define OP_Gt             80 /* same as TK_GT, synopsis: if r[P1]>r[P3] goto P2 */
#define OP_Le             81 /* same as TK_LE, synopsis: if r[P1]<=r[P3] goto P2 */
#define OP_Lt             82 /* same as TK_LT, synopsis: if r[P1]<r[P3] goto P2 */
#define OP_Ge             83 /* same as TK_GE, synopsis: if r[P1]>=r[P3] goto P2 */
#define OP_RowKey         84 /* synopsis: r[P2]=key                        */
#define OP_BitAnd         85 /* same as TK_BITAND, synopsis: r[P3]=r[P1]&r[P2] */
#define OP_BitOr          86 /* same as TK_BITOR, synopsis: r[P3]=r[P1]|r[P2] */
#define OP_ShiftLeft      87 /* same as TK_LSHIFT, synopsis: r[P3]=r[P2]<<r[P1] */
#define OP_ShiftRight     88 /* same as TK_RSHIFT, synopsis: r[P3]=r[P2]>>r[P1] */
#define OP_Add            89 /* same as TK_PLUS, synopsis: r[P3]=r[P1]+r[P2] */
#define OP_Subtract       90 /* same as TK_MINUS, synopsis: r[P3]=r[P2]-r[P1] */
#define OP_Multiply       91 /* same as TK_STAR, synopsis: r[P3]=r[P1]*r[P2] */
#define OP_Divide         92 /* same as TK_SLASH, synopsis: r[P3]=r[P2]/r[P1] */
#define OP_Remainder      93 /* same as TK_REM, synopsis: r[P3]=r[P2]%r[P1] */
#define OP_Concat         94 /* same as TK_CONCAT, synopsis: r[P3]=r[P2]+r[P1] */
#define OP_RowData        95 /* synopsis: r[P2]=data                       */
#define OP_BitNot         96 /* same as TK_BITNOT, synopsis: r[P1]= ~r[P1] */
#define OP_String8        97 /* same as TK_STRING, synopsis: r[P2]='P4'    */
#define OP_Rowid          98 /* synopsis: r[P2]=rowid                      */
#define OP_NullRow        99
#define OP_Last          100
#define OP_SorterSort    101
#define OP_Sort          102
#define OP_Rewind        103
#define OP_SorterInsert  104
#define OP_IdxInsert     105 /* synopsis: key=r[P2]                        */
#define OP_IdxDelete     106 /* synopsis: key=r[P2@P3]                     */
#define OP_IdxRowid      107 /* synopsis: r[P2]=rowid                      */
#define OP_IdxLT         108 /* synopsis: key=r[P3@P4]                     */
#define OP_IdxGE         109 /* synopsis: key=r[P3@P4]                     */
#define OP_Destroy       110
#define OP_Clear         111
#define OP_CreateIndex   112 /* synopsis: r[P2]=root iDb=P1                */
#define OP_CreateTable   113 /* synopsis: r[P2]=root iDb=P1                */
#define OP_ParseSchema   114
#define OP_LoadAnalysis  115
#define OP_DropTable     116
#define OP_DropIndex     117
#define OP_DropTrigger   118
#define OP_IntegrityCk   119
#define OP_RowSetAdd     120 /* synopsis: rowset(P1)=r[P2]                 */
#define OP_RowSetRead    121 /* synopsis: r[P3]=rowset(P1)                 */
#define OP_RowSetTest    122 /* synopsis: if r[P3] in rowset(P1) goto P2   */
#define OP_Program       123
#define OP_Param         124
#define OP_FkCounter     125 /* synopsis: fkctr[P1]+=P2                    */
#define OP_FkIfZero      126 /* synopsis: if fkctr[P1]==0 goto P2          */
#define OP_MemMax        127 /* synopsis: r[P1]=max(r[P1],r[P2])           */
#define OP_IfPos         128 /* synopsis: if r[P1]>0 goto P2               */
#define OP_IfNeg         129 /* synopsis: if r[P1]<0 goto P2               */
#define OP_IfZero        130 /* synopsis: r[P1]+=P3, if r[P1]==0 goto P2   */
#define OP_AggFinal      131 /* synopsis: accum=r[P1] N=P2                 */
#define OP_IncrVacuum    132
#define OP_Real          133 /* same as TK_FLOAT, synopsis: r[P2]=P4       */
#define OP_Expire        134
#define OP_TableLock     135 /* synopsis: iDb=P1 root=P2 write=P3          */
#define OP_VBegin        136
#define OP_VCreate       137
#define OP_VDestroy      138
#define OP_VOpen         139
#define OP_VColumn       140 /* synopsis: r[P3]=vcolumn(P2)                */
#define OP_VNext         141
#define OP_VRename       142
#define OP_ToText        143 /* same as TK_TO_TEXT                         */
#define OP_ToBlob        144 /* same as TK_TO_BLOB                         */
#define OP_ToNumeric     145 /* same as TK_TO_NUMERIC                      */
#define OP_ToInt         146 /* same as TK_TO_INT                          */
#define OP_ToReal        147 /* same as TK_TO_REAL                         */
#define OP_Pagecount     148
#define OP_MaxPgcnt      149
#define OP_Trace         150
#define OP_Noop          151
#define OP_Explain       152


/* Properties such as "out2" or "jump" that are specified in
** comments following the "case" for each opcode in the vdbe.c
** are encoded into bitvectors as follows:
*/
#define OPFLG_JUMP            0x0001  /* jump:  P2 holds jmp target */
#define OPFLG_OUT2_PRERELEASE 0x0002  /* out2-prerelease: */
#define OPFLG_IN1             0x0004  /* in1:   P1 is an input */
#define OPFLG_IN2             0x0008  /* in2:   P2 is an input */
#define OPFLG_IN3             0x0010  /* in3:   P3 is an input */
#define OPFLG_OUT2            0x0020  /* out2:  P2 is an output */
#define OPFLG_OUT3            0x0040  /* out3:  P3 is an output */
#define OPFLG_INITIALIZER {\
/*   0 */ 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x01,\
/*   8 */ 0x01, 0x01, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00,\
/*  16 */ 0x01, 0x01, 0x04, 0x24, 0x04, 0x10, 0x00, 0x02,\
/*  24 */ 0x02, 0x02, 0x02, 0x02, 0x02, 0x00, 0x00, 0x20,\
/*  32 */ 0x00, 0x00, 0x04, 0x05, 0x04, 0x00, 0x00, 0x01,\
/*  40 */ 0x01, 0x05, 0x05, 0x00, 0x00, 0x00, 0x02, 0x02,\
/*  48 */ 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,\
/*  56 */ 0x00, 0x11, 0x11, 0x11, 0x11, 0x08, 0x11, 0x11,\
/*  64 */ 0x11, 0x11, 0x02, 0x02, 0x00, 0x00, 0x00, 0x4c,\
/*  72 */ 0x4c, 0x00, 0x00, 0x00, 0x05, 0x05, 0x15, 0x15,\
/*  80 */ 0x15, 0x15, 0x15, 0x15, 0x00, 0x4c, 0x4c, 0x4c,\
/*  88 */ 0x4c, 0x4c, 0x4c, 0x4c, 0x4c, 0x4c, 0x4c, 0x00,\
/*  96 */ 0x24, 0x02, 0x02, 0x00, 0x01, 0x01, 0x01, 0x01,\
/* 104 */ 0x08, 0x08, 0x00, 0x02, 0x01, 0x01, 0x02, 0x00,\
/* 112 */ 0x02, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,\
/* 120 */ 0x0c, 0x45, 0x15, 0x01, 0x02, 0x00, 0x01, 0x08,\
/* 128 */ 0x05, 0x05, 0x05, 0x00, 0x01, 0x02, 0x00, 0x00,\
/* 136 */ 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04,\
/* 144 */ 0x04, 0x04, 0x04, 0x04, 0x02, 0x02, 0x00, 0x00,\
/* 152 */ 0x00,}

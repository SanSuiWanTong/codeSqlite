/*
	build in VS2013
	1.open_db 2.create_table 3.start transaction 
	4.1 sqlite3_prepare_v2() 4.2 sqlite3_bind_XXX() 4.3 sqlite3_step 4.4 sqlite3_reset 
	6.end transation 
	4.5 sqlite3_finalize(stmt);
	7.close_db
*/
extern "C"
{
#include "../../sqlite-amalgamation-3320100/sqlite3.h"
}
#include <stdio.h>
#include <stdlib.h>
#include <windows.h>

#define TRANSACTION_ON

#ifdef TRANSACTION_ON
#define START_TRANSACTION() sqlite3_exec(db,"begin transaction;",NULL,NULL,NULL)
#define END_TRANSACTION() sqlite3_exec(db,"commit transaction;",NULL,NULL,NULL)
#else
#define START_TRANSACTION()
#define END_TRANSACTION()
#endif

typedef enum{
	INSERT,
	SELECT,
	UPDATE,
	DEL
}SQL_OPTYPE;

typedef struct stEmployee{
	unsigned int id;
	unsigned int age;
	char registertime[26];
	float salary;
}Employee;

static unsigned int g_employeeId = 0;

sqlite3* open_db(const char* dbName){
	sqlite3* db = nullptr;
	if (dbName){
		if (SQLITE_OK != sqlite3_open(dbName, &db)){	//SQLITE_OK is 0
			printf("Open %s fail\r\n", dbName);
			return nullptr;
		}
	}
	return db;
}

int close_db(sqlite3* db){
	if (db) return sqlite3_close(db);
	return -1;
}

int create_table(sqlite3* db, const char* tableName){
	int ret = -1;	
	char* errMsg;

	char* sqlFmt = "CREATE TABLE %s (id INTEGER PRIMARY KEY,\
				                     age INTEGER,\
									 registertime VARCHAR(26),\
									 salary REAL);";
	int cmdLen = strlen(sqlFmt) + strlen(tableName) + 1;
	char* sqlCmd = (char*)malloc(cmdLen);
	if (!sqlCmd) return -1;
	memset(sqlCmd, 0, cmdLen);
	sprintf_s(sqlCmd, cmdLen, sqlFmt, tableName);
	ret = sqlite3_exec(db, sqlCmd, NULL, NULL, &errMsg);
	if (SQLITE_OK != ret){
		printf("Create table fail %s \r\n", errMsg);		
	}
	free(sqlCmd);
	return ret;
}

void init_employee(Employee* pEmployee)
{
	memset(pEmployee, 0, sizeof(*pEmployee));
	
	pEmployee->id = g_employeeId++;
	pEmployee->age = 20;
	sprintf_s(pEmployee->registertime, sizeof("2020-05-28"), "2020-05-28");
	pEmployee->salary = float(1234.56);
}

char * get_sqlCmd(const char* tableName,SQL_OPTYPE opType,Employee employee)
{
	char * insertFmt = "INSERT INTO %s values(%d, %d, '%s',%10.6f);";
	char * selectFmt = "SELECT * FROM %s where id <= %d;";
	char * updateFmt = "UPDATE %s set age=%d,registerTime='%s',salary=%10.6f where id=%d;";
	char * deleteFmt = "DELETE FROM %s where id=%d;";

	int cmdLen = strlen(insertFmt) + strlen(tableName) + sizeof(employee);
	char * sqlCmd = (char*)malloc(cmdLen);

	switch (opType)
	{
	case INSERT:
		sprintf_s(sqlCmd, cmdLen, insertFmt, tableName, employee.id, employee.age, employee.registertime, employee.salary);
		break;
	default:
		break;
	}

	return sqlCmd;
}
int test_transaction(sqlite3* db ,const char* tableName,SQL_OPTYPE optType, int count)
{
	int ret = -1;
	int failCount = 0;

	Employee employee;
	char* sqlcmd = NULL;
	char* errMsg = NULL;

	//start transaction
	START_TRANSACTION();
	for (int i = 0; i < count; i++){
		init_employee(&employee);
		sqlcmd = get_sqlCmd(tableName, optType, employee);
		if (!sqlcmd){
			failCount++;
			continue;
		}
		ret = sqlite3_exec(db, sqlcmd, NULL, NULL, &errMsg);
		if (SQLITE_OK != ret){
			failCount++;
			printf("Execute sql fail%s", sqlcmd);
		}
	}
	if (sqlcmd)	free(sqlcmd);
	if (errMsg) sqlite3_free(errMsg);

	//close transation
	END_TRANSACTION();
	return ret;
}
int test(const char* dbName, const char* tableName,\
		 SQL_OPTYPE optType, unsigned int count, int isTableExist)
{
	int ret = -1;	
	
	long t1 = GetTickCount();
	//open db
	sqlite3* db = open_db(dbName);
	if (!db) return ret;

	//create table
	if (!isTableExist){
		ret = create_table(db, tableName);
		if (SQLITE_OK != ret){
			close_db(db);
			return ret;
		}
	}
	//ret = test_transaction(db, tableName, optType, count);
	START_TRANSACTION();
	char buffer[] = "INSERT INTO testTable VALUES(?1,?2,?3,?4)";
	sqlite3_stmt* stmt;
	sqlite3_prepare_v2(db, buffer, strlen(buffer), &stmt, NULL);
	for (unsigned int i = 0; i < count; i++){
		sqlite3_bind_int(stmt, 1, g_employeeId++);
		sqlite3_bind_int(stmt, 2, 20);
		sqlite3_bind_text(stmt, 3, "2020-5-29", strlen("2020-5-29"), SQLITE_STATIC);
		sqlite3_bind_double(stmt, 4, 12345.67);
	
		if (sqlite3_step(stmt) != SQLITE_DONE){			
			printf("Commit failed!\r\n");
		}
		sqlite3_reset(stmt);
	}
	ret = END_TRANSACTION();
	sqlite3_finalize(stmt);

	//close db
	close_db(db);
	long t2 = GetTickCount();
	printf("Time cost:%dms\r\n",t2 - t1);

	return ret;
}

int main()
{
	int ret			   = -1;
	unsigned int count = 10000;	
	bool isTableExist  = true;

	char* dbName	= "testDB.db";
	char* tableName = "testTable";

	ret = test(dbName, tableName, SQL_OPTYPE::INSERT, count, isTableExist);
	if (SQLITE_OK != ret)
	{
		printf("Test failed!\r\n");
	}
	system("pause");
	
	return ret;
}
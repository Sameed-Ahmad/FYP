@echo off
REM Simple Setup for CMD (Command Prompt)
setlocal enabledelayedexpansion

echo ==========================================
echo Setting up Northwind Database
echo ==========================================
echo.

REM Add PostgreSQL to PATH for this session
set "PATH=%PATH%;C:\Program Files\PostgreSQL\18\bin"

REM Test if psql works now
psql --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Cannot find PostgreSQL 18
    echo Please make sure PostgreSQL 18 is installed at: C:\Program Files\PostgreSQL\18
    pause
    exit /b 1
)

echo [OK] PostgreSQL 18 found
psql --version

REM Check if northwind.sql exists
if not exist "northwind.sql" (
    echo [ERROR] northwind.sql not found in current directory
    echo Current directory: %CD%
    pause
    exit /b 1
)

echo [OK] Found northwind.sql
echo.

REM Get password
echo Enter PostgreSQL password for user 'postgres':
set /p DB_PASSWORD=Password: 

REM Set password for psql
set PGPASSWORD=%DB_PASSWORD%

REM Test connection
echo.
echo [INFO] Testing connection...
psql -U postgres -d postgres -c "SELECT 1;" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Connection failed. Check your password.
    pause
    exit /b 1
)
echo [OK] Connection successful!

REM Drop and create database
echo.
echo [INFO] Creating northwind database...
psql -U postgres -c "DROP DATABASE IF EXISTS northwind;" >nul 2>&1
psql -U postgres -c "CREATE DATABASE northwind;"

if errorlevel 1 (
    echo [ERROR] Failed to create database
    pause
    exit /b 1
)
echo [OK] Database created

REM Import SQL file
echo.
echo [INFO] Importing northwind.sql (please wait)...
psql -U postgres -d northwind -f northwind.sql >nul 2>&1
echo [OK] Data imported

REM Verify tables
echo.
echo [INFO] Verifying tables...
for /f %%i in ('psql -U postgres -d northwind -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';"') do set TABLE_COUNT=%%i
echo [OK] Found %TABLE_COUNT% tables

REM Show tables
echo.
echo Tables in database:
psql -U postgres -d northwind -c "\dt"

REM Create .env file
echo.
echo [INFO] Creating .env file...
(
    echo # Google Gemini Configuration ^(FREE!^)
    echo # Get your API key from: https://makersuite.google.com/app/apikey
    echo GOOGLE_API_KEY=your-gemini-api-key-here
    echo.
    echo # Database Configuration
    echo DB_HOST=localhost
    echo DB_PORT=5432
    echo DB_NAME=northwind
    echo DB_USER=postgres
    echo DB_PASSWORD=%DB_PASSWORD%
    echo.
    echo # Application Settings
    echo MAX_QUERY_RESULTS=100
    echo ENABLE_QUERY_LOGGING=true
) > .env

echo [OK] .env file created

REM Clear password
set PGPASSWORD=

echo.
echo ==========================================
echo Setup Complete!
echo ==========================================
echo.
echo [OK] Database: northwind
echo [OK] Tables: %TABLE_COUNT%
echo [OK] .env file created
echo.
echo Next Steps:
echo 1. Get FREE Gemini API key: https://makersuite.google.com/app/apikey
echo 2. Edit .env and add your GOOGLE_API_KEY
echo 3. Run: python test_system.py
echo 4. Run: python app.py
echo.
pause
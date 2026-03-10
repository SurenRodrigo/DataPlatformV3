#!/usr/bin/env python3
"""Simple MSSQL connection test script with network diagnostics."""

import pyodbc
import socket
import subprocess
import sys
import re
import urllib.request
import json

# Connection configuration
config = {
    "host": "Admmitdp.database.windows.net",
    "port": 1433,
    "database": "AdmMitDp",
    "username": "GkNrc",
    "password": "wsTQ3My7zyesYkCosBTrAuo7W!",
    "schemas": ["dbo"],
}

# Build server string with port
server = f"{config['host']},{config['port']}"

# Try both drivers
drivers_to_try = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server"
]


def extract_error_code(error_str):
    """Extract error code from error message."""
    # Look for patterns like (10054), [10054], error code 10054, etc.
    patterns = [
        r'\((\d{5})\)',  # (10054)
        r'\[(\d{5})\]',  # [10054]
        r'error code (\d{5})',  # error code 10054
        r'code (\d{5})',  # code 10054
    ]
    for pattern in patterns:
        match = re.search(pattern, str(error_str), re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def interpret_error_code(error_code):
    """Interpret common ODBC error codes."""
    error_codes = {
        '10054': {
            'name': 'Connection Reset',
            'description': 'The connection was forcibly closed by the remote host',
            'common_causes': [
                'Firewall blocking the connection',
                'Azure SQL Database firewall rules blocking this IP',
                'Network connectivity issue',
                'Server is not accessible from this network'
            ],
            'suggestions': [
                'Check Azure SQL Database firewall rules',
                'Verify the server IP/domain is correct',
                'Test network connectivity to the server'
            ]
        },
        '08001': {
            'name': 'SQL Server Network Error',
            'description': 'Unable to establish connection to SQL Server',
            'common_causes': [
                'Server name or address is incorrect',
                'Port is blocked or incorrect',
                'Network connectivity issue',
                'Firewall blocking the connection'
            ],
            'suggestions': [
                'Verify server hostname and port',
                'Check if port 1433 is accessible',
                'Test DNS resolution'
            ]
        },
        '28000': {
            'name': 'Login Failed',
            'description': 'Invalid username or password',
            'common_causes': [
                'Incorrect username',
                'Incorrect password',
                'Account is locked or disabled'
            ],
            'suggestions': [
                'Verify username and password',
                'Check if account is active'
            ]
        },
        '08S01': {
            'name': 'Communication Link Failure',
            'description': 'Communication error with the server',
            'common_causes': [
                'Network timeout',
                'Server is down or unreachable',
                'Connection dropped during handshake'
            ],
            'suggestions': [
                'Check if server is running',
                'Verify network connectivity',
                'Check firewall rules'
            ]
        }
    }
    return error_codes.get(error_code, None)


def test_dns_resolution(hostname):
    """Test DNS resolution."""
    print("  🔍 Testing DNS resolution...")
    try:
        ip_address = socket.gethostbyname(hostname)
        print(f"     ✅ DNS resolved: {hostname} → {ip_address}")
        return ip_address
    except socket.gaierror as e:
        print(f"     ❌ DNS resolution failed: {e}")
        return None


def test_port_connectivity(hostname, port, timeout=5):
    """Test if port is accessible."""
    print(f"  🔍 Testing port connectivity ({hostname}:{port})...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((hostname, port))
        sock.close()
        
        if result == 0:
            print(f"     ✅ Port {port} is OPEN and accessible")
            return True
        else:
            print(f"     ❌ Port {port} is CLOSED or BLOCKED (error code: {result})")
            return False
    except Exception as e:
        print(f"     ❌ Port test failed: {e}")
        return False


def test_network_connectivity(hostname, port):
    """Test basic network connectivity using netcat if available."""
    print(f"  🔍 Testing network connectivity using netcat...")
    try:
        result = subprocess.run(
            ['nc', '-zv', '-w', '5', hostname, str(port)],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print(f"     ✅ Network connectivity: SUCCESS")
            print(f"     {result.stdout.strip()}")
            return True
        else:
            print(f"     ❌ Network connectivity: FAILED")
            print(f"     {result.stderr.strip()}")
            return False
    except FileNotFoundError:
        print(f"     ⚠️  netcat (nc) not available, skipping...")
        return None
    except Exception as e:
        print(f"     ❌ Network test failed: {e}")
        return None


def get_outbound_ip():
    """Get the outbound/public IP address of the container."""
    print("  🔍 Detecting outbound IP address...")
    try:
        # Try multiple services for reliability
        services = [
            'https://api.ipify.org?format=json',
            'https://ifconfig.me/ip',
            'https://icanhazip.com',
        ]
        
        for service in services:
            try:
                with urllib.request.urlopen(service, timeout=5) as response:
                    if 'json' in service:
                        data = json.loads(response.read().decode())
                        ip = data.get('ip', '')
                    else:
                        ip = response.read().decode().strip()
                    
                    if ip and re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', ip):
                        print(f"     ✅ Outbound IP: {ip}")
                        print(f"     ⚠️  ADD THIS IP to Azure SQL Database firewall rules!")
                        return ip
            except Exception:
                continue
        
        print("     ⚠️  Could not determine outbound IP")
        return None
    except Exception as e:
        print(f"     ❌ Failed to get outbound IP: {e}")
        return None


def perform_network_diagnostics(hostname, port, error_code=None):
    """Perform network diagnostics based on error."""
    print()
    print("=" * 70)
    print("NETWORK DIAGNOSTICS")
    print("=" * 70)
    
    # Get outbound IP (important for Azure firewall)
    outbound_ip = get_outbound_ip()
    print()
    
    # DNS resolution test
    ip_address = test_dns_resolution(hostname)
    print()
    
    # Port connectivity test
    port_accessible = test_port_connectivity(hostname, port)
    print()
    
    # Network connectivity test (using netcat)
    network_ok = test_network_connectivity(hostname, port)
    print()
    
    # Error code interpretation
    if error_code:
        error_info = interpret_error_code(error_code)
        if error_info:
            print("=" * 70)
            print(f"ERROR CODE ANALYSIS: {error_code}")
            print("=" * 70)
            print(f"Error Name: {error_info['name']}")
            print(f"Description: {error_info['description']}")
            print()
            print("Common Causes:")
            for cause in error_info['common_causes']:
                print(f"  • {cause}")
            print()
            print("Suggestions:")
            for suggestion in error_info['suggestions']:
                print(f"  • {suggestion}")
            print()
    
    # Summary
    print("=" * 70)
    print("DIAGNOSTIC SUMMARY")
    print("=" * 70)
    if outbound_ip:
        print(f"Outbound IP: {outbound_ip} ⚠️  ADD TO AZURE FIREWALL!")
    print(f"DNS Resolution: {'✅ OK' if ip_address else '❌ FAILED'}")
    print(f"Port Connectivity: {'✅ OK' if port_accessible else '❌ FAILED'}")
    if network_ok is not None:
        print(f"Network Test (nc): {'✅ OK' if network_ok else '❌ FAILED'}")
    print()
    
    if not ip_address:
        print("⚠️  DNS resolution failed - check hostname and network connectivity")
    if not port_accessible:
        print("⚠️  Port is blocked or server is not accessible - check firewall rules")
    
    # Azure-specific guidance
    if '.database.windows.net' in hostname.lower():
        print()
        print("=" * 70)
        print("AZURE SQL DATABASE FIREWALL CONFIGURATION")
        print("=" * 70)
        if outbound_ip:
            print(f"1. Go to Azure Portal → SQL Database → Networking/Firewall")
            print(f"2. Click 'Add client IP' OR manually add this IP: {outbound_ip}")
            print(f"3. Click 'Save' to apply the firewall rule")
            print(f"4. Wait 1-2 minutes for the rule to propagate")
        else:
            print("1. Go to Azure Portal → SQL Database → Networking/Firewall")
            print("2. Find your server's outbound IP address")
            print("3. Add it to the firewall rules")
        print()
        print("Alternative: Enable 'Allow Azure services and resources'")
        print("  (Less secure but allows all Azure services to connect)")
        print()


print("Testing MSSQL connection...")
print(f"Server: {server}")
print(f"Database: {config['database']}")
print()

# List available drivers
print("Available SQL Server drivers:")
for driver in pyodbc.drivers():
    if 'SQL Server' in driver:
        print(f"  - {driver}")
print()

# Try each driver
for driver in drivers_to_try:
    if driver not in pyodbc.drivers():
        print(f"⚠️  {driver} not available, skipping...")
        continue
    
    print(f"Trying {driver}...")
    
    # Build connection string with Azure-specific settings
    # For Azure SQL Database, we need specific SSL/TLS settings
    if driver == "ODBC Driver 18 for SQL Server":
        # Driver 18 requires Encrypt=yes and has stricter requirements
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"HostNameInCertificate=*.database.windows.net;"
            f"Connection Timeout=30;"
        )
    else:
        # Driver 17 - still needs Encrypt for Azure
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )
    
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        print(f"✅ SUCCESS! Connected using {driver}")
        
        # Test query
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"SQL Server version: {version[:80]}...")
        
        cursor.close()
        conn.close()
        print("✅ Connection test PASSED!")
        exit(0)
        
    except pyodbc.Error as e:
        error_str = str(e)
        error_code = extract_error_code(error_str)
        
        print(f"❌ FAILED: {e}")
        if error_code:
            print(f"   Error Code: {error_code}")
        
        # Perform network diagnostics on failure
        perform_network_diagnostics(
            config['host'],
            config['port'],
            error_code=error_code
        )
        print()
        
    except Exception as e:
        error_str = str(e)
        error_code = extract_error_code(error_str)
        
        print(f"❌ FAILED: {e}")
        if error_code:
            print(f"   Error Code: {error_code}")
        
        # Perform network diagnostics on failure
        perform_network_diagnostics(
            config['host'],
            config['port'],
            error_code=error_code
        )
        print()

print("❌ Connection test FAILED with all drivers")
print()
print("=" * 70)
print("TROUBLESHOOTING TIPS")
print("=" * 70)
print("1. Check Azure SQL Database firewall rules:")
print("   - Azure Portal → SQL Database → Networking/Firewall")
print("   - The outbound IP was shown above in diagnostics")
print("   - Add that IP to the firewall rules")
print("   - Or enable 'Allow Azure services and resources' (less secure)")
print()
print("   IMPORTANT: Even though TCP port 1433 is open, Azure SQL Database")
print("   has APPLICATION-LEVEL firewall rules that block connections after")
print("   the initial TCP handshake. This is why netcat works but ODBC fails.")
print()
print("2. Verify connection details:")
print(f"   - Host: {config['host']}")
print(f"   - Port: {config['port']}")
print(f"   - Database: {config['database']}")
print(f"   - Username: {config['username']}")
print()
print("3. Test from container shell:")
print(f"   docker exec -it data-manager nc -zv {config['host']} {config['port']}")
print()
exit(1)

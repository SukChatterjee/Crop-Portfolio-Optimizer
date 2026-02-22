#!/usr/bin/env python3
import requests
import sys
import json
import uuid
from datetime import datetime

class CropOptimizerAPITester:
    def __init__(self, base_url="https://cropai-analytics.preview.emergentagent.com"):
        self.base_url = base_url
        self.token = None
        self.user_id = None
        self.tests_run = 0
        self.tests_passed = 0
        self.test_user_email = f"test_user_{uuid.uuid4().hex[:8]}@example.com"
        self.test_user_password = "TestPass123!"
        self.test_user_name = "Test User"

    def log(self, message):
        """Log test messages with timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {message}")

    def run_test(self, name, method, endpoint, expected_status, data=None, headers=None):
        """Run a single API test"""
        url = f"{self.base_url}/api/{endpoint}"
        test_headers = {'Content-Type': 'application/json'}
        
        if headers:
            test_headers.update(headers)
        
        if self.token and 'Authorization' not in test_headers:
            test_headers['Authorization'] = f'Bearer {self.token}'

        self.tests_run += 1
        self.log(f"🔍 Testing {name}...")
        self.log(f"   URL: {url}")
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=test_headers, timeout=30)
            elif method == 'POST':
                response = requests.post(url, json=data, headers=test_headers, timeout=30)
            elif method == 'PUT':
                response = requests.put(url, json=data, headers=test_headers, timeout=30)
            elif method == 'DELETE':
                response = requests.delete(url, headers=test_headers, timeout=30)

            success = response.status_code == expected_status
            
            if success:
                self.tests_passed += 1
                self.log(f"✅ PASSED - Status: {response.status_code}")
                try:
                    return True, response.json()
                except:
                    return True, response.text
            else:
                self.log(f"❌ FAILED - Expected {expected_status}, got {response.status_code}")
                self.log(f"   Response: {response.text[:200]}...")
                return False, {}

        except requests.exceptions.Timeout:
            self.log(f"❌ FAILED - Request timeout (30s)")
            return False, {}
        except requests.exceptions.ConnectionError:
            self.log(f"❌ FAILED - Connection error")
            return False, {}
        except Exception as e:
            self.log(f"❌ FAILED - Error: {str(e)}")
            return False, {}

    def test_root_endpoint(self):
        """Test API root endpoint"""
        success, response = self.run_test(
            "API Root",
            "GET",
            "",
            200
        )
        if success and isinstance(response, dict):
            self.log(f"   Message: {response.get('message', 'N/A')}")
            self.log(f"   Version: {response.get('version', 'N/A')}")
        return success

    def test_user_registration(self):
        """Test user registration"""
        success, response = self.run_test(
            "User Registration",
            "POST",
            "auth/register",
            200,
            data={
                "name": self.test_user_name,
                "email": self.test_user_email,
                "password": self.test_user_password
            }
        )
        
        if success and isinstance(response, dict):
            self.token = response.get('access_token')
            if 'user' in response:
                self.user_id = response['user'].get('id')
            self.log(f"   User ID: {self.user_id}")
            self.log(f"   Token received: {'Yes' if self.token else 'No'}")
        
        return success

    def test_user_login(self):
        """Test user login"""
        success, response = self.run_test(
            "User Login",
            "POST",
            "auth/login",
            200,
            data={
                "email": self.test_user_email,
                "password": self.test_user_password
            }
        )
        
        if success and isinstance(response, dict):
            login_token = response.get('access_token')
            if login_token:
                self.log(f"   Login token matches registration: {'Yes' if login_token == self.token else 'No'}")
            
        return success

    def test_get_user_profile(self):
        """Test getting current user profile"""
        success, response = self.run_test(
            "Get User Profile",
            "GET",
            "auth/me",
            200
        )
        
        if success and isinstance(response, dict):
            self.log(f"   User name: {response.get('name', 'N/A')}")
            self.log(f"   User email: {response.get('email', 'N/A')}")
        
        return success

    def test_create_analysis(self):
        """Test creating a farm analysis"""
        farm_profile = {
            "location": {
                "lat": 39.8283,
                "lng": -98.5795,
                "address": "Test Farm Location",
                "county": "Test County",
                "state": "Kansas"
            },
            "acres": 250.0,
            "has_irrigation": True,
            "soil_type": "Clay Loam",
            "soil_ph": 6.5,
            "crop_constraints": ["Rice"],
            "risk_preference": "moderate",
            "goal": "balanced"
        }

        success, response = self.run_test(
            "Create Farm Analysis",
            "POST",
            "analysis/create",
            200,
            data={"farm_profile": farm_profile}
        )
        
        if success and isinstance(response, dict):
            analysis_id = response.get('id')
            results = response.get('results', [])
            self.log(f"   Analysis ID: {analysis_id}")
            self.log(f"   Number of crop recommendations: {len(results)}")
            if results:
                top_crop = results[0]
                self.log(f"   Top recommendation: {top_crop.get('crop_name', 'N/A')}")
                self.log(f"   Expected profit: ${top_crop.get('expected_profit', 0):,.2f}")
            
            # Store for later retrieval test
            self.analysis_id = analysis_id
        
        return success

    def test_get_analysis(self):
        """Test retrieving a specific analysis"""
        if not hasattr(self, 'analysis_id'):
            self.log("   Skipping - No analysis ID from previous test")
            return False

        success, response = self.run_test(
            "Get Specific Analysis",
            "GET",
            f"analysis/{self.analysis_id}",
            200
        )
        
        if success and isinstance(response, dict):
            self.log(f"   Retrieved analysis ID: {response.get('id', 'N/A')}")
            self.log(f"   Status: {response.get('status', 'N/A')}")
        
        return success

    def test_list_analyses(self):
        """Test listing user's analyses"""
        success, response = self.run_test(
            "List User Analyses",
            "GET",
            "analysis",
            200
        )
        
        if success and isinstance(response, list):
            self.log(f"   Total analyses: {len(response)}")
            if response:
                self.log(f"   Latest analysis date: {response[0].get('created_at', 'N/A')}")
        
        return success

    def test_invalid_login(self):
        """Test login with invalid credentials"""
        success, response = self.run_test(
            "Invalid Login",
            "POST",
            "auth/login",
            401,
            data={
                "email": "invalid@example.com",
                "password": "wrongpassword"
            }
        )
        return success

    def test_unauthorized_access(self):
        """Test accessing protected endpoint without token"""
        # Temporarily remove token
        temp_token = self.token
        self.token = None
        
        success, response = self.run_test(
            "Unauthorized Access",
            "GET",
            "auth/me",
            401
        )
        
        # Restore token
        self.token = temp_token
        return success

    def run_all_tests(self):
        """Run all API tests"""
        self.log("🚀 Starting Crop Portfolio Optimizer API Tests")
        self.log(f"📍 Target URL: {self.base_url}")
        self.log("=" * 60)
        
        # Test sequence
        tests = [
            ("API Root", self.test_root_endpoint),
            ("User Registration", self.test_user_registration),
            ("User Login", self.test_user_login),
            ("Get User Profile", self.test_get_user_profile),
            ("Create Analysis", self.test_create_analysis),
            ("Get Analysis", self.test_get_analysis),
            ("List Analyses", self.test_list_analyses),
            ("Invalid Login", self.test_invalid_login),
            ("Unauthorized Access", self.test_unauthorized_access),
        ]
        
        failed_tests = []
        
        for test_name, test_func in tests:
            try:
                if not test_func():
                    failed_tests.append(test_name)
                self.log("-" * 40)
            except Exception as e:
                self.log(f"❌ {test_name} - Exception: {str(e)}")
                failed_tests.append(test_name)
                self.log("-" * 40)
        
        # Summary
        self.log("📊 TEST RESULTS SUMMARY")
        self.log(f"Total tests run: {self.tests_run}")
        self.log(f"Tests passed: {self.tests_passed}")
        self.log(f"Tests failed: {self.tests_run - self.tests_passed}")
        self.log(f"Success rate: {(self.tests_passed / self.tests_run * 100):.1f}%")
        
        if failed_tests:
            self.log(f"\n❌ Failed tests: {', '.join(failed_tests)}")
        else:
            self.log("\n✅ All tests passed!")
        
        return self.tests_passed == self.tests_run

def main():
    """Main test runner"""
    tester = CropOptimizerAPITester()
    
    try:
        success = tester.run_all_tests()
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
        return 1
    except Exception as e:
        print(f"\n💥 Fatal error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
#!/bin/bash
# Hurricane DPI — First-run setup script
# Run from the hurricane_app root directory:
#   chmod +x setup.sh && ./setup.sh

set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}  Hurricane DPI — Setup${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# ---- Check prerequisites ----

echo -e "\n${YELLOW}Checking prerequisites...${NC}"

if ! command -v node &> /dev/null; then
    echo -e "${RED}✗ Node.js not found. Install from https://nodejs.org (v18+)${NC}"
    exit 1
fi
NODE_VER=$(node -v | sed 's/v//' | cut -d. -f1)
if [ "$NODE_VER" -lt 18 ]; then
    echo -e "${RED}✗ Node.js v18+ required (found v$(node -v))${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Node.js $(node -v)${NC}"

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗ Python 3 not found. Install from https://python.org (3.10+)${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python $(python3 --version | awk '{print $2}')${NC}"

if ! command -v xcodebuild &> /dev/null; then
    echo -e "${YELLOW}⚠ Xcode CLI tools not detected. iOS simulator may not work.${NC}"
    echo -e "  Run: xcode-select --install"
else
    echo -e "${GREEN}✓ Xcode CLI tools installed${NC}"
fi

# ---- Python backend setup ----

echo -e "\n${YELLOW}Setting up Python backend...${NC}"

if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓ Created Python virtual environment${NC}"
fi

source venv/bin/activate
pip install -q -r requirements.txt
echo -e "${GREEN}✓ Python dependencies installed${NC}"

# Create .env from example if it doesn't exist
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo -e "${GREEN}✓ Created .env from template (edit to add API keys)${NC}"
else
    echo -e "${GREEN}✓ .env already exists${NC}"
fi

# ---- Mobile app setup ----

echo -e "\n${YELLOW}Setting up React Native / Expo app...${NC}"

cd mobile
npm install 2>&1 | tail -3
echo -e "${GREEN}✓ npm dependencies installed${NC}"

# iOS CocoaPods (only if Xcode available)
if command -v xcodebuild &> /dev/null; then
    echo -e "${YELLOW}Running Expo prebuild for iOS...${NC}"
    npx expo prebuild --platform ios --no-install 2>&1 | tail -3
    if [ -d "ios" ]; then
        cd ios
        if command -v pod &> /dev/null; then
            pod install 2>&1 | tail -3
            echo -e "${GREEN}✓ CocoaPods installed${NC}"
        else
            echo -e "${YELLOW}⚠ CocoaPods not found. Run: sudo gem install cocoapods${NC}"
        fi
        cd ..
    fi
fi

cd ..

# ---- Done ----

echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Setup complete!${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  ${YELLOW}To start:${NC}"
echo ""
echo -e "  ${CYAN}1. Start the API server (Terminal 1):${NC}"
echo -e "     source venv/bin/activate"
echo -e "     uvicorn main:app --reload --port 8000"
echo ""
echo -e "  ${CYAN}2. Start the Expo app (Terminal 2):${NC}"
echo -e "     cd mobile"
echo -e "     npx expo start"
echo ""
echo -e "  ${CYAN}3. Press 'i' to open in iOS Simulator${NC}"
echo -e "     Press 'w' to open in web browser"
echo -e "     Press 'a' to open in Android emulator"
echo ""
echo -e "  ${CYAN}Legacy web frontend still available at:${NC}"
echo -e "     http://localhost:8000"
echo ""

# Protocol Token Analyzer

A full-stack platform for analyzing token distribution patterns through protocol addresses, Merkl campaigns, and Disperse contracts.

## Features

- **Dynamic Protocol Analysis**: Input any protocol address to analyze token distribution in real-time
- **Merkl Integration**: Fetches campaign data from Merkl API for reward distribution analysis
- **Disperse Tracing**: Traces token flows through Disperse contracts and intermediary addresses
- **Interactive Dashboard**: View beneficiaries, intermediaries, and holdings in organized tabs
- **CSV Export**: Download complete analysis results for each data category
- **Real-time Processing**: Dynamic analysis based on user input with live feedback

## Architecture

- **Frontend**: Next.js 15 + React + Tailwind CSS + shadcn/ui components
- **Backend**: FastAPI (Python) with MongoDB integration
- **Database**: MongoDB (for transaction history, campaign data, and caching)
- **APIs**: Merkl API, Ethereum RPC (Arbitrum), Arbiscan API, Alchemy API

## Setup

### Prerequisites

- Node.js 18+
- Python 3.8+
- MongoDB instance (local or cloud)
- Required API keys (see environment variables)

### Environment Variables

Create a `.env.local` file with:

\`\`\`bash
# MongoDB
MONGO_URI=mongodb+srv://your-connection-string
DB_NAME=arb_transactions

# API Keys
ARBISCAN_API_KEY=your-arbiscan-key
ALCHEMY_KEY=your-alchemy-key
\`\`\`

### Installation

1. **Install Node.js dependencies:**
\`\`\`bash
npm install
\`\`\`

2. **Install Python dependencies:**
\`\`\`bash
pip install fastapi uvicorn pymongo pandas requests web3 python-multipart decimal
\`\`\`

### Running the Application

1. **Start the Python backend:**
\`\`\`bash
npm run python-server
# or directly: cd scripts && python api_server.py
\`\`\`

2. **Start the Next.js frontend:**
\`\`\`bash
npm run dev
\`\`\`

3. **Access the application:**
- Frontend: http://localhost:3000
- API: http://localhost:8000

## Usage

1. **Enter Protocol Address**: Input any Ethereum address (e.g., `0xa6f2B87238e54e7C3D2740e3e0b355daCbe41450`)
2. **Run Analysis**: Click "Analyze" to start the token distribution analysis
3. **View Results**: Explore the dashboard with:
   - **Summary Cards**: Total beneficiaries, intermediaries, holdings, and distributed amounts
   - **Beneficiaries Tab**: Final token recipients and amounts received
   - **Intermediaries Tab**: Addresses that processed token distributions
   - **Holdings Tab**: Current ARB token holdings for relevant addresses
4. **Export Data**: Download CSV files for each category

## API Endpoints

### FastAPI Backend (Port 8000)
- `POST /analyze` - Analyze a protocol address and return comprehensive results
- `GET /health` - Health check endpoint
- `GET /` - API status and information

### Next.js API Routes (Port 3000)
- `POST /api/analyze` - Proxy endpoint that forwards requests to Python backend

## Data Sources and Processing

- **Merkl API**: Campaign and recipient data for reward distribution analysis
- **MongoDB Collections**: 
  - Transaction history (dynamically selected based on protocol)
  - Campaign data and creator information
  - Disperse contract interaction data
- **Ethereum RPC**: On-chain balance queries and contract interactions
- **Arbiscan API**: Contract verification and source code analysis

## Technical Details

### Analysis Flow
1. **Collection Selection**: Dynamically identifies the correct MongoDB collection containing transactions for the protocol
2. **Merkl Analysis**: Fetches creator addresses, campaigns, and recipient data if applicable
3. **Disperse Tracing**: Traces token flows through Disperse contracts and intermediary addresses
4. **Data Combination**: Merges results from both analysis paths
5. **Response Formatting**: Returns structured JSON with beneficiaries, intermediaries, and holdings

### Key Features
- **Dynamic Address Handling**: No hardcoded addresses - accepts any protocol address
- **Duplicate Prevention**: Uses unique transaction identifiers to prevent double-counting
- **Balance Limiting**: Applies proper balance constraints for non-Disperse contracts
- **Real-time Processing**: Each analysis runs fresh with the provided protocol address

## Troubleshooting

- **MongoDB Connection**: Ensure your MongoDB instance is accessible and credentials are correct
- **API Keys**: Verify all required API keys are set in environment variables
- **Python Dependencies**: Make sure all Python packages are installed with correct versions
- **Port Conflicts**: Ensure ports 3000 and 8000 are available

## Development

To extend the platform:
1. **Add New Analysis Types**: Extend the `compute_beneficiaries_for_protocol` function
2. **UI Enhancements**: Modify the React components in `app/page.tsx`
3. **API Extensions**: Add new endpoints in `scripts/api_server.py`
4. **Database Schema**: Update MongoDB collections as needed

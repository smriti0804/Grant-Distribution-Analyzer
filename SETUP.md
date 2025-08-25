# Setup Guide

## Environment Configuration

Create a `.env.local` file in the root directory with the following variables:

```bash
# MongoDB Configuration
MONGO_URI=mongodb+srv://username:password@cluster.mongodb.net/
DB_NAME=arb_transactions

# API Keys
ARBISCAN_API_KEY=your-arbiscan-api-key-here
ALCHEMY_KEY=your-alchemy-api-key-here

# Optional: Override default CSV path
MERKL_CSV_PATH=./merkl_beneficiaries.csv
```

## Installation Steps

1. **Install Node.js dependencies:**
   ```bash
   npm install
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables** (see above)

4. **Start the development server:**
   ```bash
   npm run dev
   ```

## Troubleshooting

### Python Import Errors
If you encounter import errors with `parsimonious`, try:
```bash
pip uninstall parsimonious
pip install parsimonious==0.8.1
```

### MongoDB Connection
Ensure your MongoDB instance is accessible and credentials are correct.

### API Keys
Verify all required API keys are set in the `.env.local` file.

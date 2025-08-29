#!/usr/bin/env python3
"""
FastAPI server that exposes the protocol analysis functionality
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import json
from typing import Dict, List, Any
import sys
import os
import math

# Add the scripts directory to Python path to import our analyzer
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from protocol_analyzer import compute_beneficiaries_for_protocol
except Exception as e:
    print(f"Warning: Could not import protocol_analyzer: {e}")
    compute_beneficiaries_for_protocol = None

app = FastAPI(title="Protocol Analyzer API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AnalyzeRequest(BaseModel):
    protocol_addr: str

class BeneficiaryData(BaseModel):
    beneficiary_address: str
    amount_received: str

class IntermediaryData(BaseModel):
    intermediary_address: str
    amount_received: str

# class HoldingData(BaseModel):
#     address: str
#     arb_holdings: str

class AnalyzeResponse(BaseModel):
    success: bool
    beneficiaries: List[BeneficiaryData]
    intermediaries: List[IntermediaryData]
    # holdings: List[HoldingData]
    summary: Dict[str, Any]
    message: str = ""

@app.get("/")
async def root():
    return {"message": "Protocol Analyzer API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "Protocol Analyzer API"}

@app.post("/test-analyze")
async def test_analyze():
    return {
        "success": True,
        "beneficiaries": [
            {"beneficiary_address": "0x123", "amount_received": "1000"},
            {"beneficiary_address": "0x456", "amount_received": "2000"}
        ],
        "intermediaries": [
            {"intermediary_address": "0x789", "amount_received": "500"}
        ],
        # "holdings": [
        #     {"address": "0xabc", "arb_holdings": "300"}
        # ],
        "summary": {
            "total_beneficiaries": 2,
            "total_intermediaries": 1,
            # "total_holdings": 1,
            "total_amount_distributed": "3000"
        },
        "message": "Test response"
    }

@app.post("/analyze", response_model=AnalyzeResponse)
async def analyze_protocol(request: AnalyzeRequest):
    """
    Analyze a protocol address and return beneficiary and intermediary data
    """
    try:
        protocol_addr = request.protocol_addr.strip()
        
        if not protocol_addr:
            raise HTTPException(status_code=400, detail="Protocol address is required")
        
        # Validate Ethereum address format (basic check)
        if not protocol_addr.startswith("0x") or len(protocol_addr) != 42:
            raise HTTPException(status_code=400, detail="Invalid Ethereum address format")
        
        print(f"Analyzing protocol: {protocol_addr}")
        
        # Try to run the actual analysis first
        if compute_beneficiaries_for_protocol:
            try:
                result = compute_beneficiaries_for_protocol(protocol_addr)
                return {
                    "success": True,
                    "beneficiaries": result["beneficiaries"],
                    "intermediaries": result["intermediaries"],
                    # "holdings": result["holdings"],
                    "summary": result["summary"],
                    "message": "Live analysis completed"
                }
            except Exception as e:
                print(f"Live analysis failed: {e}, falling back to CSV")
        
        # Fallback: Read from CSV files
        # Beneficiaries
        beneficiaries_list = []
        try:
            df_ben = pd.read_csv("combined_beneficiaries.csv", dtype=str)
            df_ben.columns = [c.strip() for c in df_ben.columns]
            for _, row in df_ben.iterrows():
                addr = str(row.get("beneficiary_address", "")).strip()
                amt = str(row.get("amount_received", "")).strip()
                if not addr or addr.lower() in ["nan", "none"]:
                    continue
                if not amt or amt.lower() in ["nan", "none"]:
                    amt = "0"
                beneficiaries_list.append({
                    "beneficiary_address": addr,
                    "amount_received": amt
                })
        except Exception as e:
            print(f"Error reading beneficiaries CSV: {e}")

        # Intermediaries
        intermediaries_list = []
        try:
            df_int = pd.read_csv("arb_intermediaries.csv", dtype=str)
            df_int.columns = [c.strip() for c in df_int.columns]
            for _, row in df_int.iterrows():
                addr = str(row.get("intermediary_address", "")).strip()
                amt = str(row.get("amount_received", "")).strip()
                if not addr or addr.lower() in ["nan", "none"]:
                    continue
                if not amt or amt.lower() in ["nan", "none"]:
                    amt = "0"
                intermediaries_list.append({
                    "intermediary_address": addr,
                    "amount_received": amt
                })
        except Exception as e:
            print(f"Error reading intermediaries CSV: {e}")

        # Holdings
        # holdings_list = []
        # try:
        #     df_hold = pd.read_csv("arb_holdings.csv", dtype=str)
        #     df_hold.columns = [c.strip() for c in df_hold.columns]
        #     for _, row in df_hold.iterrows():
        #         addr = str(row.get("address", "")).strip()
        #         amt = str(row.get("arb_holdings", "")).strip()
        #         if not addr or addr.lower() in ["nan", "none"]:
        #             continue
        #         if not amt or amt.lower() in ["nan", "none"]:
        #             amt = "0"
        #         holdings_list.append({
        #             "address": addr,
        #             "arb_holdings": amt
        #         })
        # except Exception as e:
        #     print(f"Error reading holdings CSV: {e}")

        # Calculate total distributed
        def safe_sum_amounts(items, key):
            total = 0.0
            for i in items:
                try:
                    v = float(i.get(key, "0"))
                    if math.isnan(v):
                        continue
                    total += v
                except Exception:
                    continue
            return total

        total_distributed = safe_sum_amounts(beneficiaries_list, "amount_received")
        total_distributed += safe_sum_amounts(intermediaries_list, "amount_received")
        # total_distributed += safe_sum_amounts(holdings_list, "arb_holdings")

        # Try to read returned amounts from CSV if it exists
        total_amount_returned = "0"
        try:
            import pandas as pd
            if os.path.exists("arb_returned.csv"):
                df_ret = pd.read_csv("arb_returned.csv", dtype=str)
                df_ret.columns = [c.strip() for c in df_ret.columns]
                total_amount_returned = str(
                    sum(float(x) for x in df_ret["amount_returned"] if str(x).lower() not in ["", "nan", "none"])
                )
        except Exception as e:
            print(f"Error reading returned CSV: {e}")

        return {
            "success": True,
            "beneficiaries": beneficiaries_list,
            "intermediaries": intermediaries_list,
            # "holdings": holdings_list,
            "summary": {
                "total_beneficiaries": len(beneficiaries_list),
                "total_intermediaries": len(intermediaries_list),
                # "total_holdings": len(holdings_list),
                "total_amount_distributed": str(total_distributed),
                "total_amount_returned": str(total_amount_returned) , # <-- ensure this is present
                "protocol_address": protocol_addr
            },
            "message": "From CSV files"
        }
        
    except Exception as e:
        print(f"Error analyzing protocol {request.protocol_addr}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

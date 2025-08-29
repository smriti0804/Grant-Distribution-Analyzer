import { NextRequest, NextResponse } from "next/server";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const protocolAddr = (body?.protocol_addr ?? "").trim();

    if (!protocolAddr || !protocolAddr.startsWith("0x") || protocolAddr.length !== 42) {
      return NextResponse.json({ error: "Invalid Ethereum address format" }, { status: 400 });
    }

    console.log(`[analyze] Forwarding request to Python for: ${protocolAddr}`);

    // Call the Python API route
    const apiUrl = new URL("/api/protocol_analyzer", request.url);
    const resp = await fetch(apiUrl.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ protocol_addr: protocolAddr }),
    });

    let analysisData: any;
    try {
      const rawText = await resp.text();
      analysisData = rawText ? JSON.parse(rawText) : {};
    } catch (parseErr) {
      console.error("[analyze] Failed to parse JSON from Python response");
      return NextResponse.json(
        { error: "Invalid response from Python function" },
        { status: 502 }
      );
    }

    if (!resp.ok) {
      return NextResponse.json(
        { error: analysisData?.error || "Python function failed" },
        { status: resp.status }
      );
    }

    // Transform to frontend shape
    const transformedData = {
      beneficiaries: (analysisData.beneficiaries || []).map((b: any) => ({
        address: b.beneficiary_address,
        amount: b.amount_received,
        token: "ARB",
        percentage: "0",
      })),
      intermediaries: (analysisData.intermediaries || []).map((i: any) => ({
        address: i.intermediary_address,
        total_amount: i.amount_received,
        token: "ARB",
      })),
      summary: {
        total_beneficiaries: analysisData.summary?.total_beneficiaries ?? 0,
        total_intermediaries: analysisData.summary?.total_intermediaries ?? 0,
        total_amount: analysisData.summary?.total_amount_distributed ?? 0,
        total_amount_returned: analysisData.summary?.total_amount_returned ?? 0,
        protocol_address: protocolAddr,
      },
    };

    console.log(`[analyze] Completed: ${transformedData.summary.total_beneficiaries} beneficiaries`);
    return NextResponse.json(transformedData);

  } catch (err: any) {
    console.error("[analyze] API error:", err);
    return NextResponse.json(
      { error: err?.message || "Internal server error" },
      { status: 500 }
    );
  }
}

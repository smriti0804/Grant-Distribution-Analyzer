// app/api/analyze/route.ts
import { NextRequest, NextResponse } from "next/server";
import { spawn } from "child_process";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const protocolAddr = (body?.protocol_addr ?? "").trim();

    if (!protocolAddr || !protocolAddr.startsWith("0x") || protocolAddr.length !== 42) {
      return NextResponse.json({ error: "Invalid Ethereum address format" }, { status: 400 });
    }

    console.log(`[analyze] Running Python for: ${protocolAddr}`);

    return new Promise((resolve, reject) => {
      // Spawn Python process
      const py = spawn("python", ["scripts/protocol_analyzer.py"]);

      let result = "";
      let error = "";

      // Send request body to Python via stdin
      py.stdin.write(JSON.stringify({ protocol_addr: protocolAddr }));
      py.stdin.end();

      py.stdout.on("data", (data) => {
        result += data.toString();
      });

      py.stderr.on("data", (data) => {
        error += data.toString();
      });

      py.on("close", (code) => {
        if (code !== 0) {
          console.error("[analyze] Python error:", error);
          reject(
            NextResponse.json(
              { error: error || "Python process failed" },
              { status: 500 }
            )
          );
          return;
        }

        try {
          const analysisData = JSON.parse(result);

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
              total_beneficiaries:
                analysisData.summary?.total_beneficiaries ?? 0,
              total_intermediaries:
                analysisData.summary?.total_intermediaries ?? 0,
              total_amount:
                analysisData.summary?.total_amount_distributed ?? 0,
              total_amount_returned:
                analysisData.summary?.total_amount_returned ?? 0,
              protocol_address: protocolAddr,
            },
          };

          console.log(
            `[analyze] Completed: ${transformedData.summary.total_beneficiaries} beneficiaries`
          );
          resolve(NextResponse.json(transformedData));
        } catch (e) {
          console.error("[analyze] Failed to parse JSON from Python:", result);
          reject(
            NextResponse.json(
              { error: "Invalid JSON output from Python" },
              { status: 502 }
            )
          );
        }
      });
    });
  } catch (err: any) {
    console.error("[analyze] API error:", err);
    return NextResponse.json(
      { error: err?.message || "Internal server error" },
      { status: 500 }
    );
  }
}

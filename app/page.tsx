"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Loader2, Search, Download, TrendingUp, Users, Building2, Coins } from "lucide-react"
import { Alert, AlertDescription } from "@/components/ui/alert"

// Support flexible keys for old/new formats and allow extra fields
interface BeneficiaryData {
  beneficiary_address?: string
  address?: string
  amount_received?: string
  amount?: string
  [key: string]: any
}

interface IntermediaryData {
  intermediary_address?: string
  address?: string
  amount_received?: string
  amount?: string
  [key: string]: any
}

interface HoldingData {
  address?: string
  beneficiary_address?: string
  arb_holdings?: string
  amount_received?: string
  amount?: string
  [key: string]: any
}

interface AnalysisResult {
  success: boolean
  beneficiaries: BeneficiaryData[]
  intermediaries: IntermediaryData[]
  holdings: HoldingData[]
  summary: {
    total_beneficiaries: number
    total_intermediaries: number
    total_holdings: number
    total_amount_distributed: string
  }
  message: string
}

export default function ProtocolAnalyzer() {
  const [protocolAddress, setProtocolAddress] = useState("")
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<AnalysisResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  const analyzeProtocol = async () => {
    if (!protocolAddress.trim()) {
      setError("Please enter a protocol address")
      return
    }

    setLoading(true)
    setError(null)
    setResult(null)

    try {
      const response = await fetch("/api/analyze", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          protocol_addr: protocolAddress.trim(),
        }),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || "Analysis failed")
      }

      const data: AnalysisResult = await response.json()
      setResult(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : "An error occurred during analysis")
    } finally {
      setLoading(false)
    }
  }

  const downloadBeneficiariesCSV = () => {
    if (!result?.beneficiaries) return

    const csvContent = [
      ["beneficiary_address", "amount_received"],
      ...result.beneficiaries.map((b) => [b.beneficiary_address, b.amount_received]),
    ]
      .map((row) => row.join(","))
      .join("\n")

    const blob = new Blob([csvContent], { type: "text/csv" })
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `beneficiaries_${protocolAddress.slice(0, 8)}.csv`
    a.click()
    window.URL.revokeObjectURL(url)
  }

  const downloadIntermediariesCSV = () => {
    if (!result?.intermediaries) return

    const csvContent = [
      ["intermediary_address", "amount_received"],
      ...result.intermediaries.map((i) => [i.intermediary_address, i.amount_received]),
    ]
      .map((row) => row.join(","))
      .join("\n")

    const blob = new Blob([csvContent], { type: "text/csv" })
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `intermediaries_${protocolAddress.slice(0, 8)}.csv`
    a.click()
    window.URL.revokeObjectURL(url)
  }

  const downloadHoldingsCSV = () => {
    if (!result?.holdings) return

    const csvContent = [["address", "arb_holdings"], ...result.holdings.map((h) => [h.address, h.arb_holdings])]
      .map((row) => row.join(","))
      .join("\n")

    const blob = new Blob([csvContent], { type: "text/csv" })
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `holdings_${protocolAddress.slice(0, 8)}.csv`
    a.click()
    window.URL.revokeObjectURL(url)
  }

  // Format amount to 4 decimals, or show as string if not a number
  const formatAmount = (amount: string | undefined) => {
    if (!amount) return ""
    const num = Number(amount)
    if (isNaN(num)) return amount
    return num.toLocaleString(undefined, { maximumFractionDigits: 4, minimumFractionDigits: 0 })
  }

  const formatAddress = (address: string) => {
    return `${address.slice(0, 6)}...${address.slice(-4)}`
  }

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto px-4 py-8">
        {/* Header */}
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold mb-2">Protocol Token Analyzer</h1>
          <p className="text-muted-foreground text-lg">Analyze token distribution patterns for any protocol address</p>
        </div>

        {/* Search Section */}
        <Card className="mb-8">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Search className="h-5 w-5" />
              Protocol Analysis
            </CardTitle>
            <CardDescription>
              Enter a protocol address to analyze its token distribution through Merkl campaigns and Disperse contracts
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex gap-4">
              <Input
                placeholder="0x1234...abcd (Protocol Address)"
                value={protocolAddress}
                onChange={(e) => setProtocolAddress(e.target.value)}
                className="flex-1"
                disabled={loading}
              />
              <Button onClick={analyzeProtocol} disabled={loading || !protocolAddress.trim()} className="min-w-[120px]">
                {loading ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Analyzing
                  </>
                ) : (
                  <>
                    <Search className="mr-2 h-4 w-4" />
                    Analyze
                  </>
                )}
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Error Alert */}
        {error && (
          <Alert className="mb-6 border-destructive">
            <AlertDescription className="text-destructive">{error}</AlertDescription>
          </Alert>
        )}

        {/* Results Section */}
        {result && (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <Card>
                <CardContent className="p-6">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm font-medium text-muted-foreground">Beneficiaries</p>
                      <p className="text-2xl font-bold">{result.summary.total_beneficiaries.toLocaleString()}</p>
                    </div>
                    <Users className="h-8 w-8 text-muted-foreground" />
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-6">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm font-medium text-muted-foreground">Intermediaries</p>
                      <p className="text-2xl font-bold">{result.summary.total_intermediaries.toLocaleString()}</p>
                    </div>
                    <Building2 className="h-8 w-8 text-muted-foreground" />
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-6">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm font-medium text-muted-foreground">Holdings</p>
                      <p className="text-2xl font-bold">{result.summary.total_holdings.toLocaleString()}</p>
                    </div>
                    <Coins className="h-8 w-8 text-muted-foreground" />
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-6">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm font-medium text-muted-foreground">Total Distributed</p>
                      <p className="text-2xl font-bold">{formatAmount(result.summary.total_amount_distributed || result.summary.total_amount)}</p>
                    </div>
                    <TrendingUp className="h-8 w-8 text-muted-foreground" />
                  </div>
                </CardContent>
              </Card>
            </div>

            <Card>
              <CardHeader>
                <CardTitle>Analysis Results</CardTitle>
                <CardDescription>Detailed breakdown of token distribution data</CardDescription>
              </CardHeader>
              <CardContent>
                <Tabs defaultValue="beneficiaries" className="w-full">
                  <TabsList className="grid w-full grid-cols-3">
                    <TabsTrigger value="beneficiaries">
                      Beneficiaries ({result.summary.total_beneficiaries})
                    </TabsTrigger>
                    <TabsTrigger value="intermediaries">
                      Intermediaries ({result.summary.total_intermediaries})
                    </TabsTrigger>
                    <TabsTrigger value="holdings">Holdings ({result.summary.total_holdings})</TabsTrigger>
                  </TabsList>

                  <TabsContent value="beneficiaries" className="mt-6">
                    <div className="flex items-center justify-between mb-4">
                      <p className="text-sm text-muted-foreground">Final token recipients and their received amounts</p>
                      <Button onClick={downloadBeneficiariesCSV} variant="outline" size="sm">
                        <Download className="mr-2 h-4 w-4" />
                        Download CSV
                      </Button>
                    </div>
                    {result.beneficiaries.length > 0 ? (
                      <div className="rounded-md border">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead>Beneficiary Address</TableHead>
                              <TableHead className="text-right">Amount Received</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {result.beneficiaries
                              .slice()
                              .sort((a, b) => {
                                const aAmt = Number(a.amount_received || a.amount || 0)
                                const bAmt = Number(b.amount_received || b.amount || 0)
                                return bAmt - aAmt
                              })
                              .slice(0, 100)
                              .map((beneficiary, index) => (
                                <TableRow key={index}>
                                  <TableCell className="font-mono">{beneficiary.beneficiary_address || beneficiary.address || ""}</TableCell>
                                  <TableCell className="text-right font-mono">
                                    {formatAmount(beneficiary.amount_received || beneficiary.amount)}
                                  </TableCell>
                                </TableRow>
                              ))}
                          </TableBody>
                        </Table>
                        {result.beneficiaries.length > 100 && (
                          <div className="p-4 text-center text-sm text-muted-foreground border-t">
                            Showing first 100 results. Download CSV for complete data.
                          </div>
                        )}
                      </div>
                    ) : (
                      <div className="text-center py-8 text-muted-foreground">
                        No beneficiaries found for this protocol address.
                      </div>
                    )}
                  </TabsContent>

                  <TabsContent value="intermediaries" className="mt-6">
                    <div className="flex items-center justify-between mb-4">
                      <p className="text-sm text-muted-foreground">
                        Intermediate addresses that processed token distributions
                      </p>
                      <Button onClick={downloadIntermediariesCSV} variant="outline" size="sm">
                        <Download className="mr-2 h-4 w-4" />
                        Download CSV
                      </Button>
                    </div>
                    {result.intermediaries.length > 0 ? (
                      <div className="rounded-md border">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead>Intermediary Address</TableHead>
                              <TableHead className="text-right">Amount Processed</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {result.intermediaries.slice(0, 100).map((intermediary, index) => (
                              <TableRow key={index}>
                                <TableCell className="font-mono">{intermediary.intermediary_address || intermediary.address || ""}</TableCell>
                                <TableCell className="text-right font-mono">
                                  {formatAmount(intermediary.amount_received || intermediary.amount || intermediary.total_amount || intermediary.balance)}
                                </TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                        {result.intermediaries.length > 100 && (
                          <div className="p-4 text-center text-sm text-muted-foreground border-t">
                            Showing first 100 results. Download CSV for complete data.
                          </div>
                        )}
                      </div>
                    ) : (
                      <div className="text-center py-8 text-muted-foreground">
                        No intermediaries found for this protocol address.
                      </div>
                    )}
                  </TabsContent>

                  <TabsContent value="holdings" className="mt-6">
                    <div className="flex items-center justify-between mb-4">
                      <p className="text-sm text-muted-foreground">Addresses with current ARB token holdings</p>
                      <Button onClick={downloadHoldingsCSV} variant="outline" size="sm">
                        <Download className="mr-2 h-4 w-4" />
                        Download CSV
                      </Button>
                    </div>
                    {result.holdings.length > 0 ? (
                      <div className="rounded-md border">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead>Address</TableHead>
                              <TableHead className="text-right">ARB Holdings</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {result.holdings.slice(0, 100).map((holding, index) => (
                              <TableRow key={index}>
                                <TableCell className="font-mono">{holding.address || holding.beneficiary_address || ""}</TableCell>
                                <TableCell className="text-right font-mono">
                                  {formatAmount(holding.arb_holdings || holding.amount_received || holding.amount || holding.balance)}
                                </TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                        {result.holdings.length > 100 && (
                          <div className="p-4 text-center text-sm text-muted-foreground border-t">
                            Showing first 100 results. Download CSV for complete data.
                          </div>
                        )}
                      </div>
                    ) : (
                      <div className="text-center py-8 text-muted-foreground">
                        No holdings found for this protocol address.
                      </div>
                    )}
                  </TabsContent>
                </Tabs>
              </CardContent>
            </Card>
          </div>
        )}
      </div>
    </div>
  )
}

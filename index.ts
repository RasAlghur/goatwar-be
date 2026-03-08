/**
 * cranker.ts — GOAT WARS automated cranker
 *
 */

import * as anchor from "@coral-xyz/anchor";
import { BN } from "@coral-xyz/anchor";
import { Connection, Keypair, PublicKey, SystemProgram } from "@solana/web3.js";
import "dotenv/config";
import {
  getAssociatedTokenAddress,
  createAssociatedTokenAccountIdempotent,
  TOKEN_2022_PROGRAM_ID,
  ASSOCIATED_TOKEN_PROGRAM_ID,
} from "@solana/spl-token";
import bs58 from "bs58";
import * as fs from "fs";
import * as crypto from "crypto";
import * as path from "path";


function loadPath(): Keypair {
  const envKey = process.env.PATH_2;

  if (envKey) {
    if (envKey.trim().startsWith("[")) {
      try {
        return Keypair.fromSecretKey(new Uint8Array(JSON.parse(envKey)));
      } catch (e) {
        throw new Error(`PATH_2 looks like a JSON array but failed to parse: ${String(e)}`);
      }
    }
    // Base58 string
    try {
      return Keypair.fromSecretKey(bs58.decode(envKey));
    } catch (e) {
      throw new Error(`PATH_2 looks like base58 but failed to decode: ${String(e)}`);
    }
  }

  // File path fallback for local dev
  const keyPath = process.env.PATH_1;
  if (keyPath) {
    try {
      const raw = JSON.parse(fs.readFileSync(path.resolve(keyPath), "utf8"));
      return Keypair.fromSecretKey(new Uint8Array(raw));
    } catch (e) {
      throw new Error(`Failed to load keypair from PATH_1="${keyPath}": ${String(e)}`);
    }
  }

  throw new Error(
    "No Path found.\n" +
    "  On Railway/hosting: set PATH_2 to a JSON array string or base58.\n" +
    "  Locally: set PATH_1 to your keypair file path."
  );
}

function requireEnv(name: string): string {
  const val = process.env[name];
  if (!val) throw new Error(`Missing required env var: ${name}`);
  return val;
}

const RPC = process.env.RPC ?? "https://api.devnet.solana.com";
const PROGRAM_ID = new PublicKey(requireEnv("PROGRAM_ID"));
const MESSI_MINT = new PublicKey(requireEnv("MESSI_MINT"));
const RONALDO_MINT = new PublicKey(requireEnv("RONALDO_MINT"));
const TREASURY = new PublicKey(requireEnv("TREASURY_ADDRESS"));
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 60_000);
const ROUND_DURATION_SECS = Number(process.env.ROUND_DURATION_SECS ?? 3600);
const BREAK_DURATION_SECS = Number(process.env.BREAK_DURATION_SECS ?? 600);
const STATE_FILE_PATH = path.resolve(process.env.STATE_FILE_PATH ?? "./cranker-state.json");
const DEPLOYMENT_START_ROUND = Number(process.env.DEPLOYMENT_START_ROUND ?? 1);

const PATH_OWNER: Keypair = loadPath();

// ── State ─────────────────────────────────────────────────────────────────────
interface CrankerState {
  currentRoundNumber: number;
  updatedAt: string;
  lastKnownStatus?: { settled: boolean; randomRewardFilled: boolean };
}

function loadState(): CrankerState {
  if (fs.existsSync(STATE_FILE_PATH)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(STATE_FILE_PATH, "utf8")) as CrankerState;
      if (typeof parsed.currentRoundNumber === "number" && parsed.currentRoundNumber >= 1) {
        log(`Loaded state: round ${parsed.currentRoundNumber}`);
        return parsed;
      }
    } catch (e) { log(`State parse error: ${String(e)} — starting fresh.`); }
  }
  return { currentRoundNumber: Number(process.env.START_ROUND ?? 1), updatedAt: new Date().toISOString() };
}

function saveState(state: CrankerState): void {
  try {
    const tmp = STATE_FILE_PATH + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify({ ...state, updatedAt: new Date().toISOString() }, null, 2), "utf8");
    fs.renameSync(tmp, STATE_FILE_PATH);
  } catch (e) { error("Failed to save state:", e); }
}

// ── Anchor setup ──────────────────────────────────────────────────────────────
const connection = new Connection(RPC, "confirmed");
const wallet = new anchor.Wallet(PATH_OWNER);
const provider = new anchor.AnchorProvider(connection, wallet, { commitment: "confirmed", preflightCommitment: "confirmed" });
anchor.setProvider(provider);

const IDL_PATH = path.resolve(__dirname, "idl.json");
const idl = JSON.parse(fs.readFileSync(IDL_PATH, "utf8"));
idl.address = PROGRAM_ID.toBase58();
const program = new anchor.Program(idl, provider);

const accountClient = program.account as unknown as {
  round: { fetch: (pda: PublicKey) => Promise<RoundAccount> };
  bid: { fetch: (pda: PublicKey) => Promise<BidAccount> };
};

type RA = { pubkey: PublicKey; isWritable: boolean; isSigner: boolean };

const methodsClient = program.methods as unknown as {
  settleRound: () => {
    accounts: (a: Record<string, PublicKey>) => {
      remainingAccounts: (accs: RA[]) => { rpc: () => Promise<string> };
      rpc: () => Promise<string>;
    };
  };
  fulfillRandomWinner: (winner: PublicKey) => {
    accounts: (a: Record<string, PublicKey>) => { rpc: () => Promise<string> };
  };
  initializeRound: (roundNumber: BN, startTs: BN, endTs: BN) => {
    accounts: (a: Record<string, PublicKey>) => { rpc: () => Promise<string> };
  };
};

// ── Types ─────────────────────────────────────────────────────────────────────
interface RoundAccount {
  roundNumber: BN | number; startTs: BN | number; endTs: BN | number; settledAt: BN | number;
  mintA: PublicKey; mintB: PublicKey; escrowA: PublicKey; escrowB: PublicKey;
  totalA: BN; totalB: BN;
  highestBidA: PublicKey; highestBidAAmount: BN;
  highestBidB: PublicKey; highestBidBAmount: BN;
  settled: boolean; winnerTeam: number;
  randomRewardAmount: BN; randomRewardFilled: boolean; randomWinner: PublicKey;
  claimedRandom: boolean;
  highestBidderRewardAmount: BN; claimedHighest: boolean;
  proportionalRewardAmount: BN; proportionalRewardFilled: boolean;
  operator: PublicKey; bump: number;
}
interface BidAccount {
  bidder: PublicKey; mint: PublicKey; amount: BN;
  claimedReturn: boolean; claimedPrize: boolean;
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function sleep(ms: number) { return new Promise((res) => setTimeout(res, ms)); }

function is429(err: unknown) {
  const e = err as Record<string, unknown>;
  const msg = typeof e?.message === "string" ? e.message : String(err);
  return msg.includes("429") || msg.includes("Too Many Requests") || e?.status === 429;
}

async function withBackoff<T>(fn: () => Promise<T>, tries = 6, base = 300): Promise<T> {
  let attempt = 0;
  while (true) {
    try { return await fn(); } catch (err) {
      attempt++;
      if (!is429(err) || attempt >= tries) throw err;
      const delay = base * Math.pow(2, attempt) + Math.floor(Math.random() * base);
      log(`RPC 429 — retrying in ${delay}ms (${attempt}/${tries})`);
      await sleep(delay);
    }
  }
}

function getRoundPDA(n: number): PublicKey {
  const [pda] = PublicKey.findProgramAddressSync(
    [Buffer.from("round"), Buffer.from(new BigUint64Array([BigInt(n)]).buffer)], PROGRAM_ID);
  return pda;
}

function getBidPDA(n: number, bidder: PublicKey, mint: PublicKey): PublicKey {
  const [pda] = PublicKey.findProgramAddressSync(
    [Buffer.from("bid"), Buffer.from(new BigUint64Array([BigInt(n)]).buffer), bidder.toBuffer(), mint.toBuffer()], PROGRAM_ID);
  return pda;
}

async function fetchRound(n: number): Promise<{ account: RoundAccount; pda: PublicKey } | null> {
  const pda = getRoundPDA(n);
  const info = await withBackoff(() => connection.getAccountInfo(pda, "confirmed"));
  if (!info) return null;
  const account = await withBackoff(() => accountClient.round.fetch(pda));
  return { account, pda };
}

async function ensureAta(mint: PublicKey, owner: PublicKey): Promise<PublicKey> {
  try {
    return await createAssociatedTokenAccountIdempotent(
      connection, PATH_OWNER, mint, owner,
      { commitment: "confirmed" },
      TOKEN_2022_PROGRAM_ID,
      ASSOCIATED_TOKEN_PROGRAM_ID,
    );
  } catch (err) {
    log(`ensureAta (${owner.toBase58().slice(0, 8)}/${mint.toBase58().slice(0, 8)}): ${String((err as Error).message).slice(0, 80)}`);
    return getAssociatedTokenAddress(mint, owner, false, TOKEN_2022_PROGRAM_ID);
  }
}

// Bid account layout (must match Rust struct field order):
//   discriminator : 8  bytes  (offset  0)
//   bidder        : 32 bytes  (offset  8)
//   amount        : 16 bytes  (offset 40)  ← u128
//   mint          : 32 bytes  (offset 56)  ← filter here
//   claimed_return: 1  byte   (offset 88)
//   claimed_prize : 1  byte   (offset 89)
const BID_MINT_OFFSET = 8 + 32 + 16; // = 56

async function fetchWinningTeamBids(roundNumber: number, winningMint: PublicKey): Promise<{ bidder: PublicKey; amount: BN }[]> {
  log(`Fetching winning bids for round ${roundNumber}...`);
  try {
    const raw = await withBackoff(() => connection.getProgramAccounts(PROGRAM_ID, {
      filters: [{ memcmp: { offset: BID_MINT_OFFSET, bytes: winningMint.toBase58() } }],
      encoding: "base64",
    }));
    if (!raw.length) { log("No bid accounts found."); return []; }

    const result: { bidder: PublicKey; amount: BN }[] = [];
    for (const r of raw) {
      try {
        const bidPubkey = new PublicKey(r.pubkey);
        const bid = await withBackoff(() => accountClient.bid.fetch(bidPubkey));
        // Confirm this bid belongs to the current round
        if (getBidPDA(roundNumber, bid.bidder, winningMint).toBase58() !== bidPubkey.toBase58()) continue;
        if (bid.amount.toString() === "0") continue;
        result.push({ bidder: bid.bidder, amount: bid.amount });
      } catch (e) { log(`Skipping bid: ${String((e as Error).message).slice(0, 80)}`); }
    }
    log(`Found ${result.length} eligible bids.`);
    return result;
  } catch (err) { error("fetchWinningTeamBids failed:", err); return []; }
}

function pickWeightedRandom(bids: { bidder: PublicKey; amount: BN }[]): PublicKey | null {
  if (!bids.length) return null;
  const total = bids.reduce((sum, b) => sum + BigInt(b.amount.toString()), BigInt(0));
  if (!total) return null;
  const randBytes = crypto.randomBytes(8);
  let rand = BigInt(0);
  for (let i = 7; i >= 0; i--) rand = (rand << BigInt(8)) | BigInt(randBytes[i]);
  rand = rand % total;
  let acc = BigInt(0);
  for (const b of bids) { acc += BigInt(b.amount.toString()); if (rand < acc) return b.bidder; }
  return bids[bids.length - 1].bidder;
}

function log(...args: unknown[]) { console.log(`[${new Date().toISOString()}]`, ...args); }
function error(...args: unknown[]) { console.error(`[${new Date().toISOString()}] ERROR`, ...args); }
function formatDuration(s: number) { return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m ${s % 60}s`; }

// ── Core operations ───────────────────────────────────────────────────────────
const SETTLE_CLOCK_BUFFER_SECS = 2;

async function settleRound(roundNumber: number, account: RoundAccount, pda: PublicKey): Promise<boolean> {
  const now = Math.floor(Date.now() / 1000);
  const endTs = Number(account.endTs);
  if (now < endTs + SETTLE_CLOCK_BUFFER_SECS) {
    const wait = endTs + SETTLE_CLOCK_BUFFER_SECS - now;
    log(`Waiting ${wait}s clock buffer...`);
    await sleep(wait * 1000);
  }

  log(`Settling round ${roundNumber}...`);

  const aWins = BigInt(account.totalA.toString()) >= BigInt(account.totalB.toString());
  const losingMint = aWins ? account.mintB : account.mintA;
  const treasuryAta = await ensureAta(losingMint, TREASURY);
  log(`  Treasury ATA: ${treasuryAta.toBase58()}`);

  const remainingAccounts: RA[] = [
    { pubkey: treasuryAta, isWritable: true, isSigner: false },
  ];

  try {
    await (methodsClient.settleRound().accounts({
      round: pda,
      escrowA: account.escrowA,
      escrowB: account.escrowB,
      mintA: account.mintA,
      mintB: account.mintB,
      tokenProgram: TOKEN_2022_PROGRAM_ID,
    }) as unknown as { remainingAccounts: (accs: RA[]) => { rpc: () => Promise<string> } })
      .remainingAccounts(remainingAccounts).rpc();
    log(`Round ${roundNumber} settled.`);
    return true;
  } catch (err) { error("settleRound failed:", err); return false; }
}

async function fulfillRandomWinner(roundNumber: number, account: RoundAccount, pda: PublicKey): Promise<boolean> {
  log(`Selecting random winner for round ${roundNumber}...`);

  // No random reward allocated — nothing to fulfill
  if (account.randomRewardAmount.toString() === "0") {
    log(`Round ${roundNumber} has no random reward — skipping.`);
    return true;
  }

  const winningMint = account.winnerTeam === 1 ? account.mintA : account.mintB;
  const SYS = "11111111111111111111111111111111";

  // Exclude system/default keys AND both highest bidders (program enforces this constraint)
  const excludedAddresses = new Set([
    SYS,
    PublicKey.default.toBase58(),
    account.highestBidA.toBase58(),
    account.highestBidB.toBase58(),
  ]);

  const allBids = await fetchWinningTeamBids(roundNumber, winningMint);
  const eligibleBids = allBids.filter((b) => !excludedAddresses.has(b.bidder.toBase58()));

  log(`Eligible for random: ${eligibleBids.length} (excluded ${allBids.length - eligibleBids.length} highest/default bidders)`);

  if (eligibleBids.length === 0) {
    // Only the highest bidder participated — program forbids using them as random winner.
    // Random pot stays unclaimed this round. Auto-advance.
    log(`Round ${roundNumber}: only highest bidder on winning side — no eligible random winner. Advancing.`);
    return true;
  }

  const chosen = pickWeightedRandom(eligibleBids);
  if (!chosen) { log("pickWeightedRandom returned null — skipping."); return true; }

  log(`Random winner selected: ${chosen.toBase58()}`);

  try {
    await methodsClient.fulfillRandomWinner(chosen)
      .accounts({ round: pda, operator: PATH_OWNER.publicKey })
      .rpc();
    log(`Random winner recorded on-chain: ${chosen.toBase58()}`);
    return true;
  } catch (err) { error("fulfillRandomWinner failed:", err); return false; }
}

async function initializeRound(roundNumber: number): Promise<boolean> {
  log(`Initializing round ${roundNumber} (cooldown: ${BREAK_DURATION_SECS}s)...`);
  const now = Math.floor(Date.now() / 1000);
  const startTs = now + BREAK_DURATION_SECS;
  const endTs = startTs + ROUND_DURATION_SECS;
  const roundPda = getRoundPDA(roundNumber);

  const escrowA = await getAssociatedTokenAddress(MESSI_MINT, roundPda, true, TOKEN_2022_PROGRAM_ID);
  const escrowB = await getAssociatedTokenAddress(RONALDO_MINT, roundPda, true, TOKEN_2022_PROGRAM_ID);

  try {
    await methodsClient.initializeRound(new BN(roundNumber), new BN(startTs), new BN(endTs))
      .accounts({
        round: roundPda,
        mintA: MESSI_MINT,
        mintB: RONALDO_MINT,
        escrowA,
        escrowB,
        operator: PATH_OWNER.publicKey,
        payer: PATH_OWNER.publicKey,
        systemProgram: SystemProgram.programId,
        tokenProgram: TOKEN_2022_PROGRAM_ID,
        associatedTokenProgram: ASSOCIATED_TOKEN_PROGRAM_ID,
        rent: anchor.web3.SYSVAR_RENT_PUBKEY,
      }).rpc();
    await sleep(1200);
    log(`Round ${roundNumber} initialized | opens: ${new Date(startTs * 1000).toISOString()} | ends: ${new Date(endTs * 1000).toISOString()}`);
    return true;
  } catch (err: unknown) {
    const logs = (err as Record<string, unknown>)?.transactionLogs;
    const joined = Array.isArray(logs) ? logs.join("\n") : String(logs ?? "");
    if (joined.includes("already in use")) {
      log("initializeRound: already in use — treating as success.");
      await sleep(1500);
      return true;
    }
    error("initializeRound failed:", err);
    return false;
  }
}

// ── Tick ──────────────────────────────────────────────────────────────────────
let state = loadState();
let isProcessing = false;

async function tick(): Promise<void> {
  if (isProcessing) { log("Previous tick running — skipping"); return; }
  isProcessing = true;
  try {
    const { currentRoundNumber } = state;
    const fetched = await fetchRound(currentRoundNumber);

    if (!fetched) {
      log(`Round ${currentRoundNumber} not on-chain. Initializing...`);
      const ok = await initializeRound(currentRoundNumber);
      if (ok) { state.lastKnownStatus = { settled: false, randomRewardFilled: false }; saveState(state); }
      return;
    }

    const { account, pda } = fetched;
    const now = Math.floor(Date.now() / 1000);
    const endTs = Number(account.endTs);

    log(`Round ${currentRoundNumber} | settled: ${account.settled} | randomFilled: ${account.randomRewardFilled} | ends: ${new Date(endTs * 1000).toISOString()}`);
    state.lastKnownStatus = { settled: account.settled, randomRewardFilled: account.randomRewardFilled };
    saveState(state);

    if (now < endTs && !account.settled) { log(`Live. ${formatDuration(endTs - now)} remaining.`); return; }

    if (!account.settled) {
      const ok = await settleRound(currentRoundNumber, account, pda);
      if (!ok) return;
      const ref = await fetchRound(currentRoundNumber);
      if (!ref) return;
      Object.assign(account, ref.account);
      state.lastKnownStatus = { settled: true, randomRewardFilled: account.randomRewardFilled };
      saveState(state);
    }

    if (!account.randomRewardFilled) {
      const ok = await fulfillRandomWinner(currentRoundNumber, account, pda);
      if (!ok) return;
      const ref2 = await fetchRound(currentRoundNumber);
      if (!ref2) return;
      Object.assign(account, ref2.account);
      state.lastKnownStatus = { settled: true, randomRewardFilled: true };
      saveState(state);
    }

    const nextRound = currentRoundNumber + 1;
    log(`Round ${currentRoundNumber} complete. Initializing round ${nextRound}...`);
    const ok = await initializeRound(nextRound);
    if (ok) {
      state.currentRoundNumber = nextRound;
      state.lastKnownStatus = { settled: false, randomRewardFilled: false };
      saveState(state);
      log(`Advanced to round ${nextRound}.`);
    }
  } catch (err) {
    error("Tick error:", String((err as Error).message ?? err).slice(0, 400));
  } finally { isProcessing = false; }
}

// ── Discovery ─────────────────────────────────────────────────────────────────
async function discoverLatestRound(): Promise<number> {
  log("Scanning on-chain for latest round...");
  let latest = DEPLOYMENT_START_ROUND - 1;
  let batch = DEPLOYMENT_START_ROUND;
  const BATCH = 20;
  while (true) {
    const pdas = Array.from({ length: BATCH }, (_, i) => getRoundPDA(batch + i));
    const infos = await withBackoff(() => connection.getMultipleAccountsInfo(pdas, "confirmed"));
    let anyFound = false;
    for (let i = 0; i < infos.length; i++) {
      if (infos[i]) { anyFound = true; latest = batch + i; }
    }
    if (!anyFound) break;
    batch += BATCH;
  }
  return latest;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  log("===========================================");
  log("  GOAT WARS Cranker");
  log(`  Program  : ${PROGRAM_ID.toBase58()}`);
  log(`  Operator : ${PATH_OWNER.publicKey.toBase58()}`);
  log(`  Treasury : ${TREASURY.toBase58()}`);
  log(`  RPC      : ${RPC}`);
  log(`  Interval : ${POLL_INTERVAL_MS}ms`);
  log(`  Round    : ${ROUND_DURATION_SECS}s | Cooldown: ${BREAK_DURATION_SECS}s`);
  log(`  State    : ${STATE_FILE_PATH}`);
  log(`  Start    : round ${DEPLOYMENT_START_ROUND} (deployment floor)`);
  log(`  Token    : Token-2022 (${TOKEN_2022_PROGRAM_ID.toBase58()})`);
  log("===========================================");

  try {
    const bal = await withBackoff(() => connection.getBalance(PATH_OWNER.publicKey));
    log(`Operator SOL: ${(bal / 1e9).toFixed(4)}`);
    if (bal < 0.05e9) error("WARNING: Low SOL balance.");
  } catch (err) { error("Balance check failed:", err); }

  try {
    const onChainLatest = await discoverLatestRound();
    const floor = DEPLOYMENT_START_ROUND;
    if (onChainLatest < floor) {
      log(`No rounds >= ${floor} found on-chain. Will initialize round ${floor}.`);
      state.currentRoundNumber = floor;
      saveState(state);
    } else if (onChainLatest !== state.currentRoundNumber) {
      log(`State file says round ${state.currentRoundNumber}, but on-chain latest is round ${onChainLatest}. Syncing.`);
      state.currentRoundNumber = onChainLatest;
      saveState(state);
    } else {
      log(`State in sync with on-chain: round ${onChainLatest}.`);
    }
  } catch (err) {
    error("discoverLatestRound failed — using state file value:", err);
  }

  await tick();
  setInterval(() => void tick(), POLL_INTERVAL_MS);
}

main().catch((err) => { error("Fatal:", err); process.exit(1); });
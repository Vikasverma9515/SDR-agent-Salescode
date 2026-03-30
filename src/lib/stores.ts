import { create } from 'zustand';

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

interface ContactData {
  full_name: string;
  company: string;
  role_title?: string;
  role_bucket?: string;
  linkedin_url?: string;
  linkedin_verified?: boolean;
  email?: string;
  email_status?: string;
  domain?: string;
}

interface DiscoveredCandidate {
  index: number;
  full_name: string;
  role_title: string;
  company: string;
  linkedin_url: string;
  linkedin_verified: boolean;
  source: string;
  pre_selected: boolean;
  group: 'matched' | 'bonus';
  importance_note?: string;
  is_new?: boolean;
}

interface ContactSelectionEvent {
  company: string;
  candidates: DiscoveredCandidate[];
  matched_count: number;
  bonus_count: number;
  total: number;
  timeout_secs: number;
}

interface RoleBucket {
  id: string;
  label: string;
  count: number;
  sample_roles: string[];
  pre_selected: boolean;
  priority_rank?: number;
  priority_reason?: string;
}

interface RoleSelectionEvent {
  company: string;
  buckets: RoleBucket[];
  total_found: number;
}

interface ScoutCandidate {
  full_name: string;
  role_title: string;
  company: string;
  linkedin_url: string;
  linkedin_verified: boolean;
  linkedin_status?: string;         // CONFIRMED | UNCONFIRMED
  employment_verified?: string;     // CONFIRMED | UNCERTAIN | REJECTED
  title_match?: string;             // MATCH | MISMATCH | UNKNOWN
  actual_title?: string;
  email?: string;
  email_status?: string;            // valid | catch-all | unknown | invalid
  buying_role?: string;             // Decision Maker | Influencer
  source: string;
  confidence: string;
  // Duplicate detection
  exists_in_sheet?: boolean;
  sheet_name?: string;
  sheet_row?: number;
  // Company context (passed at commit time)
  company_domain?: string;
  company_account_type?: string;
  company_account_size?: string;
  added?: boolean;
  sendStatus?: 'idle' | 'sending' | 'sent' | 'error' | 'duplicate';
}

interface ScoutMessage {
  role: 'user' | 'assistant';
  content: string;
  candidates?: ScoutCandidate[];
}

// ---------------------------------------------------------------------------
// Fini Store
// ---------------------------------------------------------------------------

type CardStatus = 'pending' | 'sending' | 'sent' | 'skipped' | 'error';

interface ReviewCard {
  data: any;
  status: CardStatus;
  errorMsg?: string;
  editCompanyName: string;
  editRawName: string;
  editSalesNavUrl: string;
  editDomain: string;
  editSdrAssigned: string;
  editEmailFormat: string;
  editAccountType: string;
  editAccountSize: string;
  isCandidate?: boolean;
  groupId?: string;
}

interface FiniState {
  // Form
  companies: string;
  sdr: string;
  region: string;
  submitN8n: boolean;
  autoMode: boolean;
  // Run
  running: boolean;
  threadId: string | null;
  error: string | null;
  cancelled: boolean;
  paused: boolean;
  // Results
  reviewCards: ReviewCard[];
  enrichmentDone: boolean;
  enrichmentStats: { processed: number; errors: string[] } | null;
  enrichProgress: Record<string, string>;
  elapsedSecs: number;
  isSendingAll: boolean;
  // Actions
  set: (patch: Partial<Omit<FiniState, 'set' | 'reset'>>) => void;
  reset: () => void;
}

const finiDefaults = {
  companies: '',
  sdr: 'Amy',
  region: '',
  submitN8n: false,
  autoMode: false,
  running: false,
  threadId: null as string | null,
  error: null as string | null,
  cancelled: false,
  paused: false,
  reviewCards: [] as ReviewCard[],
  enrichmentDone: false,
  enrichmentStats: null as { processed: number; errors: string[] } | null,
  enrichProgress: {} as Record<string, string>,
  elapsedSecs: 0,
  isSendingAll: false,
};

export const useFiniStore = create<FiniState>((set) => ({
  ...finiDefaults,
  set: (patch) => set((s) => ({ ...s, ...patch })),
  reset: () => set((s) => ({ ...s, ...finiDefaults })),
}));

// ---------------------------------------------------------------------------
// Searcher Store
// ---------------------------------------------------------------------------

const DEFAULT_DM_ROLES = 'VP Ecommerce,CDO,Head of Digital,CTO,CMO,VP Marketing,VP Sales';

interface SearcherState {
  // Form
  companies: string;
  dmRoles: string;
  autoMode: boolean;
  autoVeri: boolean;
  // Run
  running: boolean;
  threadId: string | null;
  error: string | null;
  cancelled: boolean;
  paused: boolean;
  result: any;
  veriThreadId: string | null;
  // Progress
  scanProgress: Record<string, string>;
  elapsedSecs: number;
  scanDone: boolean;
  // Live feed
  liveContacts: ContactData[];
  recentActivity: string[];
  // Role selection (step 1)
  roleEvent: RoleSelectionEvent | null;
  selectedBuckets: string[];
  roleSubmitting: boolean;
  // Contact selection (step 2)
  selectionEvent: ContactSelectionEvent | null;
  selectedIndices: number[];
  selSubmitting: boolean;
  // UI
  contactTab: 'matched' | 'bonus' | 'all';
  leftTab: 'config' | 'scout';
  findMorePrompt: string;
  findMoreLoading: boolean;
  // AI Scout
  scoutMessages: ScoutMessage[];
  scoutAdded: ScoutCandidate[];
  // Actions
  set: (patch: Partial<Omit<SearcherState, 'set' | 'resetRun'>>) => void;
  resetRun: () => void;
}

const searcherRunDefaults = {
  running: false,
  threadId: null as string | null,
  error: null as string | null,
  cancelled: false,
  paused: false,
  result: null as any,
  veriThreadId: null as string | null,
  scanProgress: {} as Record<string, string>,
  elapsedSecs: 0,
  scanDone: false,
  liveContacts: [] as ContactData[],
  recentActivity: [] as string[],
  roleEvent: null as RoleSelectionEvent | null,
  selectedBuckets: [] as string[],
  roleSubmitting: false,
  selectionEvent: null as ContactSelectionEvent | null,
  selectedIndices: [] as number[],
  selSubmitting: false,
  contactTab: 'matched' as 'matched' | 'bonus' | 'all',
  findMorePrompt: '',
  findMoreLoading: false,
};

export const useSearcherStore = create<SearcherState>((set) => ({
  // Form (persists across runs)
  companies: '',
  dmRoles: DEFAULT_DM_ROLES,
  autoMode: false,
  autoVeri: true,
  leftTab: 'config' as 'config' | 'scout',
  scoutMessages: [] as ScoutMessage[],
  scoutAdded: [] as ScoutCandidate[],
  ...searcherRunDefaults,
  set: (patch) => set((s) => ({ ...s, ...patch })),
  resetRun: () => set((s) => ({ ...s, ...searcherRunDefaults })),
}));

// ---------------------------------------------------------------------------
// Veri Store
// ---------------------------------------------------------------------------

interface VeriState {
  running: boolean;
  threadId: string | null;
  error: string | null;
  cancelled: boolean;
  paused: boolean;
  result: any;
  rowStart: string;
  rowEnd: string;
  set: (patch: Partial<Omit<VeriState, 'set' | 'reset'>>) => void;
  reset: () => void;
}

const veriDefaults = {
  running: false,
  threadId: null as string | null,
  error: null as string | null,
  cancelled: false,
  paused: false,
  result: null as any,
  rowStart: '',
  rowEnd: '',
};

export const useVeriStore = create<VeriState>((set) => ({
  ...veriDefaults,
  set: (patch) => set((s) => ({ ...s, ...patch })),
  reset: () => set((s) => ({ ...s, ...veriDefaults })),
}));

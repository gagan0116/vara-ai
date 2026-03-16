/**
 * Shared HLD Pipeline Diagram Modal
 * Dynamically injects the HLD modal HTML and handles all interaction logic.
 * Include this script on any page that needs the "How it works" modal.
 */
(function () {
    'use strict';

    // ── Modal HTML Template ──────────────────────────────────────────
    const HLD_MODAL_HTML = `
    <div class="hld-modal-overlay hidden" id="hldModal">
        <div class="hld-modal-content">
            <div class="hld-modal-header">
                <div class="hld-modal-title">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="24" height="24">
                        <circle cx="18" cy="5" r="3" />
                        <circle cx="6" cy="12" r="3" />
                        <circle cx="18" cy="19" r="3" />
                        <line x1="8.59" y1="13.51" x2="15.42" y2="17.49" />
                        <line x1="15.41" y1="6.51" x2="8.59" y2="10.49" />
                    </svg>
                    <h2>How VARA-AI Works</h2>
                </div>
                <p class="hld-modal-subtitle">Click on any step to learn more about the process</p>
                <button class="hld-modal-close" id="hldModalClose">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <line x1="18" y1="6" x2="6" y2="18" />
                        <line x1="6" y1="6" x2="18" y2="18" />
                    </svg>
                </button>
            </div>

            <div class="hld-modal-body">
                <div class="hld-pipeline-container">
                    <svg class="hld-flow-lines" viewBox="0 0 1400 100" preserveAspectRatio="none">
                        <defs>
                            <linearGradient id="flowGradient1" x1="0%" y1="0%" x2="100%" y2="0%">
                                <stop offset="0%" style="stop-color:#6366f1;stop-opacity:1" />
                                <stop offset="50%" style="stop-color:#8b5cf6;stop-opacity:1" />
                                <stop offset="100%" style="stop-color:#06b6d4;stop-opacity:1" />
                            </linearGradient>
                            <linearGradient id="flowGradient2" x1="0%" y1="0%" x2="100%" y2="0%">
                                <stop offset="0%" style="stop-color:#8b5cf6;stop-opacity:1" />
                                <stop offset="100%" style="stop-color:#22c55e;stop-opacity:1" />
                            </linearGradient>
                            <filter id="glow">
                                <feGaussianBlur stdDeviation="2" result="coloredBlur" />
                                <feMerge>
                                    <feMergeNode in="coloredBlur" />
                                    <feMergeNode in="SourceGraphic" />
                                </feMerge>
                            </filter>
                        </defs>
                        <path class="flow-path" d="M 100 50 L 300 50" stroke="url(#flowGradient1)" stroke-width="2" fill="none" filter="url(#glow)" />
                        <path class="flow-path" d="M 300 50 L 500 50" stroke="url(#flowGradient1)" stroke-width="2" fill="none" filter="url(#glow)" />
                        <path class="flow-path" d="M 500 50 L 700 50" stroke="url(#flowGradient1)" stroke-width="2" fill="none" filter="url(#glow)" />
                        <path class="flow-path" d="M 700 50 L 900 50" stroke="url(#flowGradient2)" stroke-width="2" fill="none" filter="url(#glow)" />
                        <path class="flow-path" d="M 900 50 L 1100 50" stroke="url(#flowGradient2)" stroke-width="2" fill="none" filter="url(#glow)" />
                        <path class="flow-path" d="M 1100 50 L 1300 50" stroke="url(#flowGradient2)" stroke-width="2" fill="none" filter="url(#glow)" />
                        <circle r="4" fill="#6366f1" filter="url(#glow)"><animateMotion dur="2s" repeatCount="indefinite" path="M 100 50 L 300 50" /></circle>
                        <circle r="4" fill="#8b5cf6" filter="url(#glow)"><animateMotion dur="2s" repeatCount="indefinite" begin="0.33s" path="M 300 50 L 500 50" /></circle>
                        <circle r="4" fill="#6366f1" filter="url(#glow)"><animateMotion dur="2s" repeatCount="indefinite" begin="0.66s" path="M 500 50 L 700 50" /></circle>
                        <circle r="4" fill="#06b6d4" filter="url(#glow)"><animateMotion dur="2s" repeatCount="indefinite" begin="1s" path="M 700 50 L 900 50" /></circle>
                        <circle r="4" fill="#8b5cf6" filter="url(#glow)"><animateMotion dur="2s" repeatCount="indefinite" begin="1.33s" path="M 900 50 L 1100 50" /></circle>
                        <circle r="4" fill="#22c55e" filter="url(#glow)"><animateMotion dur="2s" repeatCount="indefinite" begin="1.66s" path="M 1100 50 L 1300 50" /></circle>
                    </svg>

                    <div class="hld-steps-row">
                        <div class="hld-step" data-step="email" tabindex="0">
                            <div class="hld-step-icon email">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z" />
                                    <polyline points="22,6 12,13 2,6" />
                                </svg>
                            </div>
                            <div class="hld-step-label">Customer Email</div>
                            <div class="hld-step-badge input">INPUT</div>
                            <div class="hld-step-tooltip">
                                <h4>📧 Where It All Starts</h4>
                                <p>A customer sends an email asking for help with a return, refund, or product replacement. They can attach their receipt or photos of damaged items.</p>
                                <div class="tooltip-details">
                                    <span class="detail-item">📎 Attach receipts & photos</span>
                                    <span class="detail-item">✉️ Just send an email!</span>
                                </div>
                            </div>
                        </div>

                        <div class="hld-step" data-step="gmail" tabindex="0">
                            <div class="hld-step-icon gmail">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" />
                                    <path d="M13.73 21a2 2 0 0 1-3.46 0" />
                                </svg>
                            </div>
                            <div class="hld-step-label">Gmail Monitoring</div>
                            <div class="hld-step-badge trigger">TRIGGER</div>
                            <div class="hld-step-tooltip">
                                <h4>🔔 Always Watching the Inbox</h4>
                                <p>Our system automatically monitors the support inbox 24/7. The moment a new email arrives, we spring into action — no waiting, no delays!</p>
                                <div class="tooltip-details">
                                    <span class="detail-item">⚡ Instant detection</span>
                                    <span class="detail-item">🕐 Works 24/7</span>
                                </div>
                            </div>
                        </div>

                        <div class="hld-step" data-step="classify" tabindex="0">
                            <div class="hld-step-icon classify">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <polygon points="12 2 2 7 12 12 22 7 12 2" />
                                    <polyline points="2 17 12 22 22 17" />
                                    <polyline points="2 12 12 17 22 12" />
                                </svg>
                            </div>
                            <div class="hld-step-label">AI Classification</div>
                            <div class="hld-step-badge nova">AMAZON NOVA</div>
                            <div class="hld-step-tooltip">
                                <h4>🧠 Understanding the Request</h4>
                                <p>Our AI reads the email like a human would and figures out what the customer needs. Is it a return? A refund? A replacement? The AI understands the intent.</p>
                                <div class="tooltip-details">
                                    <span class="detail-item category-return">📦 Return</span>
                                    <span class="detail-item category-refund">💰 Refund</span>
                                    <span class="detail-item category-replace">🔄 Replace</span>
                                </div>
                            </div>
                        </div>

                        <div class="hld-step" data-step="docs" tabindex="0">
                            <div class="hld-step-icon docs">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                                    <polyline points="14 2 14 8 20 8" />
                                    <line x1="16" y1="13" x2="8" y2="13" />
                                    <line x1="16" y1="17" x2="8" y2="17" />
                                </svg>
                            </div>
                            <div class="hld-step-label">Document Processing</div>
                            <div class="hld-step-badge mcp">MCP SERVER</div>
                            <div class="hld-step-tooltip">
                                <h4>📄 Reading Attachments</h4>
                                <p>The AI opens and reads any attached files — like scanning a receipt to find the order number, date, and items purchased. It can also look at photos to assess product damage.</p>
                                <div class="tooltip-details">
                                    <span class="detail-item">🧾 Reads PDF receipts</span>
                                    <span class="detail-item">📷 Analyzes photos</span>
                                </div>
                            </div>
                        </div>

                        <div class="hld-step" data-step="database" tabindex="0">
                            <div class="hld-step-icon database">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <ellipse cx="12" cy="5" rx="9" ry="3" />
                                    <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
                                    <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
                                </svg>
                            </div>
                            <div class="hld-step-label">Database Verification</div>
                            <div class="hld-step-badge postgres">POSTGRESQL</div>
                            <div class="hld-step-tooltip">
                                <h4>🔍 Checking the Records</h4>
                                <p>We verify the customer's information against our order database. Did they really buy this item? When? How much did they pay? This ensures everything matches up.</p>
                                <div class="tooltip-details">
                                    <span class="detail-item">✅ Confirms purchase</span>
                                    <span class="detail-item">📋 Matches order details</span>
                                </div>
                            </div>
                        </div>

                        <div class="hld-step" data-step="policy" tabindex="0">
                            <div class="hld-step-icon policy">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <circle cx="18" cy="5" r="3" />
                                    <circle cx="6" cy="12" r="3" />
                                    <circle cx="18" cy="19" r="3" />
                                    <line x1="8.59" y1="13.51" x2="15.42" y2="17.49" />
                                    <line x1="15.41" y1="6.51" x2="8.59" y2="10.49" />
                                </svg>
                            </div>
                            <div class="hld-step-label">Policy Engine</div>
                            <div class="hld-step-badge neo4j">NEO4J</div>
                            <div class="hld-step-tooltip">
                                <h4>📖 Consulting the Rulebook</h4>
                                <p>Like a smart assistant who memorized the entire return policy! The AI checks all company rules — return windows, eligible items, exceptions — to make a fair decision.</p>
                                <div class="tooltip-details">
                                    <span class="detail-item">📅 Return deadlines</span>
                                    <span class="detail-item">📜 Policy rules</span>
                                    <span class="detail-item">⚖️ Fair decisions</span>
                                </div>
                            </div>
                        </div>

                        <div class="hld-step" data-step="decision" tabindex="0">
                            <div class="hld-step-icon decision">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
                                    <polyline points="22 4 12 14.01 9 11.01" />
                                </svg>
                            </div>
                            <div class="hld-step-label">Decision & Response</div>
                            <div class="hld-step-badge output">OUTPUT</div>
                            <div class="hld-step-tooltip">
                                <h4>✅ The Final Answer</h4>
                                <p>The AI makes a decision and explains it clearly to the customer. Every decision comes with a reason, so customers understand exactly why their request was approved or denied.</p>
                                <div class="tooltip-details">
                                    <span class="detail-item decision-approve">✓ Approved</span>
                                    <span class="detail-item decision-deny">✗ Denied</span>
                                    <span class="detail-item decision-review">👀 Needs Review</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="hld-legend">
                    <h4>Technology Stack</h4>
                    <div class="legend-items">
                        <div class="legend-item">python</div>
                        <div class="legend-item">amazon-nova-api</div>
                        <div class="legend-item">fastapi</div>
                        <div class="legend-item">mcp</div>
                        <div class="legend-item">postgresql</div>
                        <div class="legend-item">google-cloud-sql</div>
                        <div class="legend-item">google-cloud</div>
                        <div class="legend-item">cloud-run</div>
                        <div class="legend-item">pubsub</div>
                        <div class="legend-item">cloud-build</div>
                        <div class="legend-item">firestore</div>
                        <div class="legend-item">docker</div>
                        <div class="legend-item">neo4j-aura</div>
                        <div class="legend-item">pypdf</div>
                        <div class="legend-item">oauth</div>
                        <div class="legend-item">sse-starlette</div>
                        <div class="legend-item">llama-parse</div>
                        <div class="legend-item">cloud-tasks</div>
                        <div class="legend-item">gmail-api</div>
                        <div class="legend-item">uvicorn</div>
                        <div class="legend-item">multi-agent-system</div>
                        <div class="legend-item">beautiful-soup</div>
                        <div class="legend-item">javascript</div>
                        <div class="legend-item">openai-sdk</div>
                        <div class="legend-item">asyncio</div>
                        <div class="legend-item">fastmcp</div>
                    </div>
                </div>
            </div>
        </div>
    </div>`;

    // ── Inject Modal HTML ────────────────────────────────────────────
    function injectModal() {
        // Don't inject twice
        if (document.getElementById('hldModal')) return;
        document.body.insertAdjacentHTML('beforeend', HLD_MODAL_HTML);
    }

    // ── Modal Logic ──────────────────────────────────────────────────
    function initHLDModal() {
        const hldModal = document.getElementById('hldModal');
        const hldModalClose = document.getElementById('hldModalClose');

        if (!hldModal) return;

        // Close button
        if (hldModalClose) {
            hldModalClose.addEventListener('click', closeHLDModal);
        }

        // Close on overlay click
        hldModal.addEventListener('click', (e) => {
            if (e.target === hldModal) {
                closeHLDModal();
            }
        });

        // Close on Escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !hldModal.classList.contains('hidden')) {
                closeHLDModal();
            }
        });

        // Step interaction
        const steps = hldModal.querySelectorAll('.hld-step');
        steps.forEach((step, index) => {
            step.addEventListener('click', (e) => {
                e.stopPropagation();
                const isActive = step.classList.contains('active');
                steps.forEach(s => s.classList.remove('active'));
                if (!isActive) {
                    step.classList.add('active');
                    if (window.innerWidth <= 1200) {
                        setTimeout(() => {
                            step.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                        }, 100);
                    }
                }
            });

            step.addEventListener('mouseenter', () => {
                if (window.innerWidth > 1200) {
                    steps.forEach(s => { if (s !== step) s.classList.remove('active'); });
                }
            });

            step.addEventListener('keydown', (e) => {
                if (e.key === 'ArrowRight' && index < steps.length - 1) {
                    steps[index + 1].focus();
                } else if (e.key === 'ArrowLeft' && index > 0) {
                    steps[index - 1].focus();
                } else if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    step.click();
                }
            });
        });

        // Close tooltip when clicking outside any step
        hldModal.addEventListener('click', (e) => {
            if (!e.target.closest('.hld-step')) {
                steps.forEach(s => s.classList.remove('active'));
            }
        });

        // Wire up the "How it works" help link from navbar
        const helpLink = document.getElementById('helpLink');
        if (helpLink) {
            helpLink.addEventListener('click', (e) => {
                e.preventDefault();
                openHLDModal();
            });
        }
    }

    // ── Public API (attached to window) ──────────────────────────────
    window.openHLDModal = function () {
        const hldModal = document.getElementById('hldModal');
        if (hldModal) {
            hldModal.classList.remove('hidden');
            document.body.style.overflow = 'hidden';

            const firstStep = hldModal.querySelector('.hld-step');
            if (firstStep) {
                hldModal.querySelectorAll('.hld-step.active').forEach(s => s.classList.remove('active'));
                setTimeout(() => {
                    firstStep.classList.add('active');
                    firstStep.focus();
                }, 300);
            }
        }
    };

    window.closeHLDModal = function () {
        const hldModal = document.getElementById('hldModal');
        if (hldModal) {
            hldModal.classList.add('hidden');
            document.body.style.overflow = '';
            hldModal.querySelectorAll('.hld-step.active').forEach(s => s.classList.remove('active'));
        }
    };

    // ── Initialize on DOM Ready ──────────────────────────────────────
    document.addEventListener('DOMContentLoaded', () => {
        injectModal();
        initHLDModal();
    });
})();

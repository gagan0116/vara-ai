/**
 * Customer Support AI Demo - Frontend Application
 * Dynamically loads scenarios from /scenarios/*.json
 */

// ============================================
// Demo Scenarios - Loaded dynamically
// ============================================
let demoScenarios = {};

// ============================================
// Load scenarios from JSON files
// ============================================
async function loadScenarios() {
    const scenarioIds = ['scenario1', 'scenario2', 'scenario3', 'scenario4'];

    for (const id of scenarioIds) {
        try {
            const url = `scenarios/${id}/${id}.json`;
            const response = await fetch(url);
            if (response.ok) {
                const data = await response.json();
                demoScenarios[id] = data;
            }
        } catch (error) {
            // Silently skip failed scenario loads
        }
    }

    return demoScenarios;
}

// ============================================
// Populate dropdown with loaded scenarios
// ============================================
function populateScenarioDropdown() {
    const select = document.getElementById('scenarioSelect');
    if (!select) return;

    // Clear existing options except the first placeholder
    while (select.options.length > 1) {
        select.remove(1);
    }

    // Add options for each loaded scenario
    for (const [id, scenario] of Object.entries(demoScenarios)) {
        const option = document.createElement('option');
        option.value = id;

        // Create a descriptive label based on scenario data
        const categoryEmoji = {
            'RETURN': '📦',
            'REFUND': '💰',
            'REPLACEMENT': '🔄'
        };
        const emoji = categoryEmoji[scenario.category] || '📧';
        // Use date in label instead of email (PII)
        const dateStr = new Date(scenario.received_at).toLocaleDateString();
        option.textContent = `${emoji} ${scenario.category} Request (${dateStr})`;

        select.appendChild(option);
    }
}

// ============================================
// State Management
// ============================================
const state = {
    selectedScenario: null,
    isProcessing: false,
    pipelineStep: 0,
    startTime: null,
    timerInterval: null,
    extractedData: null,   // Stores LLM extraction result from SSE
    verifiedData: null     // Stores DB verification result from SSE
};

// ============================================
// DOM Elements
// ============================================
const elements = {
    // Scenario Selection
    scenarioSelect: document.getElementById('scenarioSelect'),

    // Email Preview
    emailPreview: document.getElementById('emailPreview'),
    emptyState: document.getElementById('emptyState'),
    receivedAt: document.getElementById('receivedAt'),
    emailBody: document.getElementById('emailBody'),
    attachmentsList: document.getElementById('attachmentsList'),

    // Submit
    submitBtn: document.getElementById('submitBtn'),

    // Pipeline
    pipelineSteps: document.querySelectorAll('.pipeline-step'),
    pipelineTimer: document.getElementById('pipelineTimer'),
    timerValue: document.getElementById('timerValue'),

    // Pipeline Results
    resultCategory: document.getElementById('resultCategory'),
    resultConfidenceFill: document.getElementById('resultConfidenceFill'),
    resultConfidenceValue: document.getElementById('resultConfidenceValue'),
    parsedFiles: document.getElementById('parsedFiles'),
    defectAnalysis: document.getElementById('defectAnalysis'),

    // Logs
    logsContainer: document.getElementById('logsContainer'),
    logsToggle: document.getElementById('logsToggle'),

    // Modal
    jsonModal: document.getElementById('jsonModal'),
    modalTitle: document.getElementById('modalTitle'),
    jsonContent: document.getElementById('jsonContent'),
    modalClose: document.getElementById('modalClose'),
    closeModalBtn: document.getElementById('closeModalBtn'),
    copyJsonBtn: document.getElementById('copyJsonBtn'),

    // View JSON Buttons
    viewJsonBtns: document.querySelectorAll('.view-json-btn')
};

// ============================================
// Event Listeners
// ============================================
function initializeEventListeners() {
    // Scenario Selection
    elements.scenarioSelect.addEventListener('change', handleScenarioChange);

    // Submit Button
    elements.submitBtn.addEventListener('click', handleSubmit);

    // Logs Toggle
    elements.logsToggle.addEventListener('click', toggleLogs);

    // Modal
    elements.modalClose.addEventListener('click', closeModal);
    elements.closeModalBtn.addEventListener('click', closeModal);
    elements.copyJsonBtn.addEventListener('click', copyJsonToClipboard);
    elements.jsonModal.addEventListener('click', (e) => {
        if (e.target === elements.jsonModal) closeModal();
    });

    // View JSON Buttons (using event delegation for dynamic buttons)
    document.addEventListener('click', (e) => {
        if (e.target.closest('.view-json-btn')) {
            const btn = e.target.closest('.view-json-btn');
            handleViewJson(btn.dataset.json);
        }
    });

    // Keyboard shortcuts
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') closeModal();
    });
}

// ============================================
// Handler Functions
// ============================================
function handleScenarioChange(e) {
    const scenarioId = e.target.value;

    if (!scenarioId) {
        // No scenario selected
        elements.emailPreview.classList.add('hidden');
        elements.emptyState.classList.remove('hidden');
        elements.submitBtn.disabled = true;
        state.selectedScenario = null;
        return;
    }

    const scenario = demoScenarios[scenarioId];
    if (scenario) {
        state.selectedScenario = scenario;
        populateEmailPreview(scenario);

        addLog('info', `Scenario loaded: ${scenario.category} request from ${scenario.user_id}`);
    }
}

function populateEmailPreview(scenario) {
    // Show email preview, hide empty state
    elements.emailPreview.classList.remove('hidden');
    elements.emptyState.classList.add('hidden');
    elements.submitBtn.disabled = false;

    // Category badge (Removed)
    // elements.categoryBadge.textContent = scenario.category;
    // elements.categoryBadge.className = `category-badge ${scenario.category.toLowerCase()}`;

    // Received timestamp
    const receivedDate = new Date(scenario.received_at);
    elements.receivedAt.textContent = receivedDate.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'long',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true
    });

    // Confidence (Removed)
    // const confidencePercent = Math.round(scenario.confidence * 100);
    // elements.confidenceFill.style.width = `${confidencePercent}%`;
    // elements.confidenceValue.textContent = `${confidencePercent}%`;

    // Email body - clean up the duplicated text if present
    let emailText = scenario.email_body || '';

    // The email_body often has duplicate content (formatted + plain text concatenated)
    // Look for the signature pattern and take content up to and including first signature
    const signatureMatch = emailText.match(/Best regards,[\r\n]+[A-Za-z\s]+/);
    if (signatureMatch) {
        const firstSignatureEnd = emailText.indexOf(signatureMatch[0]) + signatureMatch[0].length;
        // Check if there's substantial content after the signature (indicating duplication)
        const afterSignature = emailText.substring(firstSignatureEnd).trim();
        if (afterSignature.length > 50) {
            // There's duplicated content after - truncate to just the formatted part
            emailText = emailText.substring(0, firstSignatureEnd);
        }
    }

    // Clean up extra whitespace
    emailText = emailText.replace(/(\r\n){3,}/g, '\r\n\r\n').trim();
    elements.emailBody.textContent = emailText;

    // Attachments
    renderAttachments(scenario.attachments);
}

function renderAttachments(attachments) {
    if (!attachments || attachments.length === 0) {
        elements.attachmentsList.innerHTML = `
            <span class="no-attachments">No attachments</span>
        `;
        return;
    }

    elements.attachmentsList.innerHTML = attachments.map(att => {
        const isPdf = att.mimeType === 'application/pdf';
        const iconClass = isPdf ? 'file-icon' : 'image-icon';
        const iconSvg = isPdf
            ? `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                <polyline points="14 2 14 8 20 8"/>
               </svg>`
            : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
                <circle cx="8.5" cy="8.5" r="1.5"/>
                <polyline points="21 15 16 10 5 21"/>
               </svg>`;

        // Create Blob URL for base64 data
        let fileUrl = '#';
        if (att.data && att.data.__type__ === 'bytes' && att.data.encoding === 'base64') {
            try {
                const blob = base64ToBlob(att.data.data, att.mimeType);
                fileUrl = URL.createObjectURL(blob);
            } catch (e) {
                console.error('Error creating blob for attachment:', e);
            }
        } else if (att.path) {
            fileUrl = att.path;
        }

        return `
            <a href="${fileUrl}" target="_blank" class="attachment-chip" title="Open in new tab">
                <span class="${iconClass}">${iconSvg}</span>
                <span>${att.filename}</span>
            </a>
        `;
    }).join('');
}

// Helper to convert base64 to Blob
function base64ToBlob(base64, mimeType) {
    const byteCharacters = atob(base64);
    const byteArrays = [];

    for (let offset = 0; offset < byteCharacters.length; offset += 512) {
        const slice = byteCharacters.slice(offset, offset + 512);
        const byteNumbers = new Array(slice.length);

        for (let i = 0; i < slice.length; i++) {
            byteNumbers[i] = slice.charCodeAt(i);
        }

        const byteArray = new Uint8Array(byteNumbers);
        byteArrays.push(byteArray);
    }

    return new Blob(byteArrays, { type: mimeType });
}

async function handleSubmit() {
    if (state.isProcessing || !state.selectedScenario) return;

    // Reset pipeline to clear previous scenario results
    resetPipeline();

    // Start processing
    state.isProcessing = true;
    state.pipelineStep = 0;

    // Update button state
    elements.submitBtn.querySelector('.btn-content').classList.add('hidden');
    elements.submitBtn.querySelector('.btn-loader').classList.remove('hidden');
    elements.submitBtn.disabled = true;

    // Start timer
    startTimer();

    addLog('info', '🚀 Starting email processing pipeline...');

    // Simulate pipeline execution
    await runPipeline();
}

async function runPipeline() {
    const scenario = state.selectedScenario; // Already the scenario object

    // API endpoint - Cloud Run backend
    const API_URL = 'https://nmpyyppm3p.us-east-1.awsapprunner.com/process-demo';

    // Reset all steps to pending
    document.querySelectorAll('.pipeline-step').forEach(step => {
        step.classList.remove('active', 'completed', 'error');
        const status = step.querySelector('.step-status');
        if (status) {
            status.textContent = 'Pending';
            status.className = 'step-status pending';
        }
        const result = step.querySelector('.step-result');
        if (result) result.classList.add('hidden');
    });

    addLog('info', '🌐 Connecting to backend...');

    try {
        const response = await fetch(API_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(scenario)
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        addLog('success', '✅ Connected to Cloud Run backend');

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        let lastDecision = null;

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });

            // Parse SSE events from buffer
            const lines = buffer.split('\n');
            buffer = lines.pop() || ''; // Keep incomplete line in buffer

            for (const line of lines) {
                if (line.startsWith('data:')) {
                    try {
                        const jsonStr = line.substring(5).trim();
                        if (!jsonStr) continue;

                        const event = JSON.parse(jsonStr);

                        // Handle progress events
                        if (event.step && event.status) {
                            const stepElement = document.querySelector(`[data-step="${event.step}"]`);

                            if (stepElement) {
                                const statusEl = stepElement.querySelector('.step-status');

                                if (event.status === 'active') {
                                    stepElement.classList.add('active');
                                    stepElement.classList.remove('completed', 'error');
                                    if (statusEl) {
                                        statusEl.textContent = 'Processing...';
                                        statusEl.className = 'step-status processing';
                                    }
                                    // Show step result for adjudication to reveal substeps
                                    if (event.step === 'adjudication') {
                                        const resultEl = stepElement.querySelector('.step-result');
                                        if (resultEl) resultEl.classList.remove('hidden');
                                    }
                                    // Show step result for verification to reveal dynamic substeps
                                    if (event.step === 'verification') {
                                        const resultEl = stepElement.querySelector('.step-result');
                                        if (resultEl) resultEl.classList.remove('hidden');
                                    }
                                } else if (event.status === 'complete') {
                                    stepElement.classList.remove('active');
                                    stepElement.classList.add('completed');
                                    if (statusEl) {
                                        statusEl.textContent = 'Completed';
                                        statusEl.className = 'step-status completed';
                                    }
                                    // Show step result if available
                                    const resultEl = stepElement.querySelector('.step-result');
                                    if (resultEl) resultEl.classList.remove('hidden');
                                    // Show verification result card when complete
                                    if (event.step === 'verification') {
                                        const verificationResult = document.getElementById('verificationResult');
                                        if (verificationResult) verificationResult.style.display = 'flex';
                                    }
                                } else if (event.status === 'error') {
                                    stepElement.classList.remove('active');
                                    stepElement.classList.add('error');
                                    if (statusEl) {
                                        statusEl.textContent = 'Error';
                                        statusEl.className = 'step-status error';
                                    }
                                }
                            }

                            // Handle substep updates for adjudication
                            if (event.step === 'adjudication' && event.substep && event.substep_status) {
                                updateSubstep(event.substep, event.substep_status, event.log, event.data);
                            }

                            // Handle substep updates for verification (dynamic)
                            if (event.step === 'verification' && event.substep && event.substep_status) {
                                updateVerificationSubstep(event.substep, event.substep_status, event.log, event.data);
                            }


                            // Add log
                            if (event.log) {
                                const logType = event.status === 'error' ? 'error' :
                                    event.status === 'complete' ? 'success' : 'info';
                                addLog(logType, event.log);
                            }

                            // Update UI with real data
                            if (event.data) {
                                updateStepData(event.step, event.data);

                                // Capture extraction result for modal display
                                if (event.step === 'extraction' && event.status === 'complete') {
                                    state.extractedData = event.data;
                                }

                                // Capture verification result for modal display
                                if (event.step === 'verification' && event.status === 'complete') {
                                    state.verifiedData = event.data;
                                }

                                // Capture decision for final message
                                if (event.step === 'decision' && event.data.decision) {
                                    lastDecision = event.data;
                                }
                            }
                        }
                    } catch (e) {
                        console.warn('Failed to parse SSE event:', e, line);
                    }
                }
            }
        }

        // Pipeline complete
        stopTimer();
        state.isProcessing = false;

        elements.submitBtn.querySelector('.btn-content').classList.remove('hidden');
        elements.submitBtn.querySelector('.btn-loader').classList.add('hidden');
        elements.submitBtn.disabled = false;

        if (lastDecision) {
            addLog('success', `🎉 Pipeline completed! Decision: ${lastDecision.decision}`);
        } else {
            addLog('success', '🎉 Pipeline completed!');
        }

    } catch (error) {
        console.error('Pipeline error:', error);
        addLog('error', `❌ Pipeline failed: ${error.message}`);

        stopTimer();
        state.isProcessing = false;

        elements.submitBtn.querySelector('.btn-content').classList.remove('hidden');
        elements.submitBtn.querySelector('.btn-loader').classList.add('hidden');
        elements.submitBtn.disabled = false;
    }
}

// Helper function to update step data in the UI
function updateStepData(step, data) {
    if (!data) return;

    switch (step) {
        case 'classification':
            if (data.category) {
                elements.resultCategory.textContent = data.category;
                elements.resultCategory.className = `result-value category-${data.category.toLowerCase()}`;
            }
            if (data.confidence !== undefined) {
                const conf = Math.round(data.confidence * 100);
                elements.resultConfidenceFill.style.width = `${conf}%`;
                elements.resultConfidenceValue.textContent = `${conf}%`;
            }
            break;

        case 'parsing':
            if (data.filename && elements.parsedFiles) {
                const existing = elements.parsedFiles.innerHTML || '';
                elements.parsedFiles.innerHTML = existing + `
                    <div class="result-file">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <polyline points="20 6 9 17 4 12"/>
                        </svg>
                        <span>${data.filename}</span>
                    </div>
                `;
            }
            break;

        case 'defect':
            if (data.analysis && elements.defectAnalysis) {
                elements.defectAnalysis.innerHTML = `
                    <div class="defect-result">
                        <span class="defect-status">✓ Image analyzed</span>
                        <span class="defect-detail">${data.analysis.substring(0, 150)}...</span>
                    </div>
                `;
            }
            break;

        case 'extraction':
            if (data.order_invoice_id || data.customer_email) {
                // Could update extraction details panel if it exists
            }
            break;

        case 'verification':
            // Handle fuzzy match / manual review case with suggested order
            if (data.suggested_order) {
                state.verifiedData = data.suggested_order;
                state.verifiedData._needs_review = true;
                state.verifiedData._fuzzy_tools = data.fuzzy_tools_used;
                state.verifiedData._confidence = data.confidence || 'low';

                // Update verification result UI to show review state
                const verificationResult = document.getElementById('verificationResult');
                if (verificationResult) {
                    verificationResult.classList.add('review');
                    verificationResult.classList.remove('success');
                    const statusEl = verificationResult.querySelector('.verification-status');
                    const detailsEl = verificationResult.querySelector('.verification-details');
                    const iconEl = verificationResult.querySelector('.verification-icon');
                    const successIcon = verificationResult.querySelector('.icon-success');
                    const reviewIcon = verificationResult.querySelector('.icon-review');

                    if (statusEl) statusEl.textContent = 'Suggested Order - Review Required';
                    if (detailsEl) detailsEl.textContent = `Found via: ${data.fuzzy_tools_used.join(', ')} | Confidence: ${data.confidence || 'low'}`;
                    if (iconEl) {
                        iconEl.classList.remove('success');
                        iconEl.classList.add('review');
                    }
                    // Show warning icon, hide success icon
                    if (successIcon) successIcon.style.display = 'none';
                    if (reviewIcon) reviewIcon.style.display = 'block';
                }
            } else if (data.order_id) {
                // Direct match - could update verification details panel if it exists
            }
            break;

        case 'decision':
            if (data.decision) {
                // Update the decision text (don't overwrite the whole container!)
                const decisionTextEl = document.getElementById('decisionText');
                const decisionContainer = document.getElementById('finalDecision');
                const reasoningEl = document.getElementById('decisionReasoning');
                const explanationEl = document.getElementById('customerExplanation');

                if (decisionTextEl) {
                    decisionTextEl.textContent = data.decision;
                }
                if (decisionContainer) {
                    decisionContainer.classList.remove('approved', 'denied', 'review');
                    decisionContainer.classList.add(data.decision.toLowerCase());
                }
                if (reasoningEl && data.reasoning) {
                    reasoningEl.textContent = data.reasoning;
                }
                // Check for customer explanation in nested adjudication data
                if (explanationEl && data.adjudication && data.adjudication.customer_explanation) {
                    explanationEl.textContent = data.adjudication.customer_explanation;
                }
            }
            break;
    }
}

// Helper function to update adjudication sub-steps in real-time
function updateSubstep(substepId, status, log, data) {
    const substep = document.querySelector(`[data-substep="${substepId}"]`);
    if (!substep) return;

    const indicator = substep.querySelector('.substep-indicator');
    const statusEl = substep.querySelector('.substep-status');

    // Remove all state classes
    substep.classList.remove('active', 'complete', 'pending');
    if (indicator) {
        indicator.classList.remove('active', 'complete', 'pending');
    }

    if (status === 'active') {
        substep.classList.add('active');
        if (indicator) {
            indicator.classList.add('active');
            indicator.textContent = '●';
        }
        if (statusEl) {
            statusEl.textContent = 'Running...';
        }
    } else if (status === 'complete') {
        substep.classList.add('complete');
        if (indicator) {
            indicator.classList.add('complete');
            indicator.textContent = '✓';
        }
        if (statusEl) {
            // Show the log message as the status (e.g., category name, decision)
            statusEl.textContent = log || 'Done';
        }

        // Handle decision substep - populate reasoning
        if (substepId === 'decision' && data) {
            const reasoningEl = document.getElementById('decisionReasoning');
            const decisionTextEl = document.getElementById('decisionText');
            const decisionContainer = document.getElementById('finalDecision');

            if (reasoningEl && data.reasoning) {
                reasoningEl.textContent = data.reasoning;
            }
            if (decisionTextEl && data.decision) {
                decisionTextEl.textContent = data.decision;
                // Update styling based on decision
                if (decisionContainer) {
                    decisionContainer.classList.remove('approved', 'denied', 'review');
                    decisionContainer.classList.add(data.decision.toLowerCase());
                }
            }
        }

        // Handle explain substep - populate customer explanation
        if (substepId === 'explain' && data) {
            const explanationEl = document.getElementById('customerExplanation');
            if (explanationEl && data.explanation) {
                explanationEl.textContent = data.explanation;
            }
        }
    }
}

// Helper function to reset all adjudication sub-steps
function resetSubsteps() {
    const substeps = document.querySelectorAll('.substep');
    substeps.forEach(substep => {
        substep.classList.remove('active', 'complete');
        const indicator = substep.querySelector('.substep-indicator');
        const statusEl = substep.querySelector('.substep-status');
        if (indicator) {
            indicator.classList.remove('active', 'complete');
            indicator.classList.add('pending');
            indicator.textContent = '○';
        }
        if (statusEl) {
            statusEl.textContent = 'Pending';
        }
    });
}

// ============================================
// Verification Sub-Steps (Dynamic)
// ============================================

// Track verification sub-steps state
const verificationSubsteps = {};

// Add or update a verification sub-step dynamically
function updateVerificationSubstep(substepId, status, log, data) {
    const container = document.getElementById('verificationSubsteps');
    if (!container) return;

    // Get or create the sub-step element
    let substepEl = document.getElementById(`verification-substep-${substepId}`);

    if (!substepEl) {
        // Create new sub-step element
        substepEl = document.createElement('div');
        substepEl.className = 'substep';
        substepEl.id = `verification-substep-${substepId}`;

        // Determine icon based on substep type
        let icon = '🔧';
        if (substepId === 'init') icon = '⚙️';
        else if (substepId === 'llm_think') icon = '🧠';
        else if (substepId === 'complete') icon = '✅';
        else if (substepId === 'error') icon = '❌';
        else if (substepId.startsWith('tool_')) icon = '🔧';

        substepEl.innerHTML = `
            <span class="substep-indicator pending">○</span>
            <span class="substep-icon">${icon}</span>
            <span class="substep-label">${log || substepId}</span>
            <span class="substep-status">Pending</span>
        `;

        container.appendChild(substepEl);
        verificationSubsteps[substepId] = substepEl;
    }

    const indicator = substepEl.querySelector('.substep-indicator');
    const label = substepEl.querySelector('.substep-label');
    const statusEl = substepEl.querySelector('.substep-status');

    // Update label if log changed
    if (log && label) {
        label.textContent = log;
    }

    // Update status
    if (status === 'active') {
        substepEl.classList.remove('complete');
        substepEl.classList.add('active');
        if (indicator) {
            indicator.classList.remove('pending', 'complete');
            indicator.classList.add('active');
            indicator.textContent = '●';
        }
        if (statusEl) {
            statusEl.textContent = 'In Progress';
        }
    } else if (status === 'complete') {
        substepEl.classList.remove('active');
        substepEl.classList.add('complete');
        if (indicator) {
            indicator.classList.remove('pending', 'active');
            indicator.classList.add('complete');
            indicator.textContent = '✓';
        }
        if (statusEl) {
            statusEl.textContent = log || 'Done';
        }
    } else if (status === 'error') {
        substepEl.classList.remove('active');
        substepEl.classList.add('error');
        if (indicator) {
            indicator.classList.remove('pending', 'active', 'complete');
            indicator.classList.add('error');
            indicator.textContent = '✗';
        }
        if (statusEl) {
            statusEl.textContent = log || 'Error';
        }
    }
}

// Reset verification sub-steps container
function resetVerificationSubsteps() {
    const container = document.getElementById('verificationSubsteps');
    if (container) {
        container.innerHTML = '';
    }
    // Clear tracking
    Object.keys(verificationSubsteps).forEach(key => delete verificationSubsteps[key]);

    // Hide and reset the verification result
    const resultEl = document.getElementById('verificationResult');
    if (resultEl) {
        resultEl.style.display = 'none';
        resultEl.classList.remove('review');

        // Reset icon state
        const iconEl = resultEl.querySelector('.verification-icon');
        if (iconEl) {
            iconEl.classList.remove('review');
            iconEl.classList.add('success');
        }

        // Reset icon visibility (show success, hide review)
        const successIcon = resultEl.querySelector('.icon-success');
        const reviewIcon = resultEl.querySelector('.icon-review');
        if (successIcon) successIcon.style.display = 'block';
        if (reviewIcon) reviewIcon.style.display = 'none';

        // Reset text
        const statusEl = resultEl.querySelector('.verification-status');
        const detailsEl = resultEl.querySelector('.verification-details');
        if (statusEl) statusEl.textContent = 'Order Verified';
        if (detailsEl) detailsEl.textContent = 'Customer & order matched in database';
    }
}

// ============================================
// Timer Functions
// ============================================
function startTimer() {
    state.startTime = Date.now();
    elements.pipelineTimer.classList.remove('hidden');

    state.timerInterval = setInterval(() => {
        const elapsed = Date.now() - state.startTime;
        const seconds = Math.floor(elapsed / 1000);
        const minutes = Math.floor(seconds / 60);
        const secs = seconds % 60;
        elements.timerValue.textContent = `${String(minutes).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
    }, 100);
}

function stopTimer() {
    if (state.timerInterval) {
        clearInterval(state.timerInterval);
        state.timerInterval = null;
    }
}

// ============================================
// Logs Functions
// ============================================
function toggleLogs() {
    elements.logsContainer.classList.toggle('collapsed');
    elements.logsToggle.classList.toggle('collapsed');
}

function addLog(type, message) {
    const time = new Date().toLocaleTimeString('en-US', {
        hour12: false,
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });

    const logEntry = document.createElement('div');
    logEntry.className = `log-entry ${type}`;
    logEntry.innerHTML = `
        <span class="log-time">${time}</span>
        <span class="log-message">${message}</span>
    `;

    elements.logsContainer.appendChild(logEntry);
    elements.logsContainer.scrollTop = elements.logsContainer.scrollHeight;
}

// ============================================
// Modal Functions
// ============================================
function handleViewJson(type) {
    let data;
    let title;

    if (type === 'extracted') {
        // Show the actual extraction result from LLM (captured from SSE events)
        if (state.extractedData) {
            data = state.extractedData;
            title = 'Extracted Order Data (from LLM)';
        } else {
            // Fallback to scenario metadata if extraction hasn't run yet
            data = {
                note: 'Run the pipeline to see extraction results',
                category: state.selectedScenario?.category,
                user_id: state.selectedScenario?.user_id
            };
            title = 'Extracted Email Data (pending)';
        }
    } else {
        // Show the actual verified record from database (captured from SSE events)
        if (state.verifiedData) {
            data = state.verifiedData;
            // Check if this is a suggested order needing review
            if (state.verifiedData._needs_review) {
                title = `Suggested Order (Needs Review) - Confidence: ${state.verifiedData._confidence || 'low'}`;
            } else {
                title = 'Verified Order Data (from Database)';
            }
        } else {
            // Fallback if verification hasn't run yet
            data = {
                note: 'Run the pipeline to see verification results',
                status: 'pending'
            };
            title = 'Verified Order Data (pending)';
        }
    }

    elements.modalTitle.textContent = title;
    elements.jsonContent.querySelector('code').textContent = JSON.stringify(data, null, 2);
    elements.jsonModal.classList.remove('hidden');
}

function closeModal() {
    elements.jsonModal.classList.add('hidden');
}

function copyJsonToClipboard() {
    const json = elements.jsonContent.querySelector('code').textContent;
    navigator.clipboard.writeText(json).then(() => {
        const originalText = elements.copyJsonBtn.innerHTML;
        elements.copyJsonBtn.innerHTML = `
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                <polyline points="20 6 9 17 4 12"/>
            </svg>
            Copied!
        `;
        setTimeout(() => {
            elements.copyJsonBtn.innerHTML = originalText;
        }, 2000);
    });
}

// ============================================
// Pipeline Functions
// ============================================
function resetPipeline() {
    elements.pipelineSteps.forEach(step => {
        step.classList.remove('active', 'completed', 'error');
        step.querySelector('.step-status').textContent = 'Pending';
        step.querySelector('.step-status').className = 'step-status pending';

        const resultElement = step.querySelector('.step-result');
        if (resultElement) {
            resultElement.classList.add('hidden');
        }
    });

    elements.pipelineTimer.classList.add('hidden');
    elements.timerValue.textContent = '00:00';

    // Clear parsed files from previous scenario
    if (elements.parsedFiles) {
        elements.parsedFiles.innerHTML = '';
    }

    // Reset defect analysis to default state
    if (elements.defectAnalysis) {
        elements.defectAnalysis.innerHTML = '<span class="no-images-text">No defect images in this request</span>';
    }

    // Reset adjudication sub-steps
    resetSubsteps();

    // Reset verification sub-steps (dynamic)
    resetVerificationSubsteps();

    // Reset captured SSE data for modals
    state.extractedData = null;
    state.verifiedData = null;
}

// ============================================
// Utility Functions
// ============================================
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================
// Initialize Application
// ============================================
async function init() {
    // Load scenarios from JSON files
    await loadScenarios();

    // Populate dropdown with loaded scenarios
    populateScenarioDropdown();

    initializeEventListeners();
    addLog('info', 'System initialized. Select a demo scenario to begin...');
}

// Start the application when DOM is ready
document.addEventListener('DOMContentLoaded', init);

// ============================================
// Tooltip System - Tap to Reveal
// ============================================
const TooltipManager = {
    overlay: null,
    bubble: null,
    activeIcon: null,

    // Tooltip content for each component
    tooltips: {
        // Email Pipeline Steps
        'classification': {
            title: 'Email Classification',
            text: 'AI analyzes the email to determine intent (RETURN, REFUND, or REPLACEMENT) and extracts key details using natural language processing.'
        },
        'parsing': {
            title: 'Document Parsing',
            text: 'LlamaParse extracts text from attached PDF invoices to verify purchase details like order ID, date, and items.'
        },
        'defect': {
            title: 'Defect Analysis',
            text: 'Amazon Nova Vision examines any product images to detect and describe visible defects or damage.'
        },
        'extraction': {
            title: 'LLM Data Extraction',
            text: 'Amazon Nova extracts structured data (customer name, invoice ID, product details) from the email and attachments.'
        },
        'verification': {
            title: 'Database Verification',
            text: 'MCP agent queries your database using SQL tools to verify the order exists and matches customer claims.'
        },
        'adjudication': {
            title: 'Policy Adjudication',
            text: 'Traverses the policy knowledge graph to find applicable rules and determine if the request meets policy criteria.'
        },
        'decision': {
            title: 'Final Decision',
            text: 'Combines all previous analysis to generate an APPROVED, DENIED, or NEEDS REVIEW decision with detailed reasoning.'
        },
        // Sub-elements
        'confidence': {
            title: 'Confidence Score',
            text: 'Indicates how certain the AI is about its classification (0-100%). Higher scores mean stronger confidence in the detected intent.'
        },
        'extracted-btn': {
            title: 'Extracted Data',
            text: 'Shows the raw JSON data extracted by the LLM, including customer info, order details, and request specifics.'
        },
        'verified-btn': {
            title: 'Verified Order',
            text: 'Displays the order record retrieved from database lookup, confirming the customer\'s purchase history.'
        },
        'scenario': {
            title: 'Demo Scenarios',
            text: 'Choose from pre-loaded demo scenarios featuring sample customer emails with various request types (return, refund, replacement).'
        },
        // Adjudication sub-steps
        'substep-context': {
            title: 'Build Context',
            text: 'Gathers all relevant information about the request to prepare for policy evaluation.'
        },
        'substep-classify': {
            title: 'Classify Category',
            text: 'Determines which policy category applies to this specific request type.'
        },
        'substep-graph': {
            title: 'Graph Traversal',
            text: 'Navigates the policy knowledge graph to find relevant rules, conditions, and thresholds.'
        },
        'substep-sources': {
            title: 'Fetch Citations',
            text: 'Retrieves the source policy documents that support the decision for transparency.'
        },
        'substep-decision': {
            title: 'LLM Decision',
            text: 'AI makes the final adjudication decision based on gathered context and policy rules.'
        },
        'substep-explain': {
            title: 'Generate Explanation',
            text: 'Creates a clear, customer-friendly explanation of the decision and next steps.'
        }
    },

    init() {
        // Create overlay element
        this.overlay = document.createElement('div');
        this.overlay.className = 'tooltip-overlay hidden';
        document.body.appendChild(this.overlay);

        // Create bubble element
        this.bubble = document.createElement('div');
        this.bubble.className = 'tooltip-bubble';
        document.body.appendChild(this.bubble);

        // Track hover state
        this.hoverTimeout = null;
        this.hideTimeout = null;
        this.isHovering = false;
        this.triggeredByClick = false;

        // Event listeners
        this.overlay.addEventListener('click', () => this.forceHide());
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') this.forceHide();
        });

        // Close tooltip when clicking anywhere outside
        document.addEventListener('click', (e) => {
            if (this.triggeredByClick && this.activeIcon) {
                // Check if click is outside the tooltip and icon
                const clickedIcon = e.target.closest('.info-icon');
                const clickedBubble = e.target.closest('.tooltip-bubble');

                if (!clickedIcon && !clickedBubble) {
                    this.forceHide();
                }
            }
        });

        // Attach click and hover handlers to all info icons
        document.querySelectorAll('.info-icon').forEach(icon => {
            // Click to toggle (for mobile and accessibility)
            icon.addEventListener('click', (e) => this.toggle(e, icon));
            icon.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    this.toggle(e, icon);
                }
            });

            // Hover to show (desktop)
            icon.addEventListener('mouseenter', () => {
                this.isHovering = true;
                // Clear any pending hide
                if (this.hideTimeout) {
                    clearTimeout(this.hideTimeout);
                }
                // Small delay to prevent accidental triggers
                this.hoverTimeout = setTimeout(() => {
                    if (this.isHovering) {
                        this.show(icon, false);
                    }
                }, 200);
            });

            icon.addEventListener('mouseleave', () => {
                this.isHovering = false;
                if (this.hoverTimeout) {
                    clearTimeout(this.hoverTimeout);
                }
                // Hide immediately when hover-triggered
                if (!this.triggeredByClick) {
                    this.hide();
                }
            });
        });
    },

    toggle(event, icon) {
        event.stopPropagation();

        if (this.activeIcon === icon && this.triggeredByClick) {
            this.forceHide();
        } else {
            this.triggeredByClick = true;
            this.show(icon, true);
        }
    },

    // Position tooltip using document-relative coords (position:absolute on body)
    _position(icon) {
        const rect = icon.getBoundingClientRect();
        const bubbleWidth = 320;
        const bubbleHeight = this.bubble.offsetHeight;
        const padding = 12;

        // Convert viewport coords to document coords
        let left = rect.left + window.scrollX;
        let top = rect.bottom + padding + window.scrollY;

        // Reset arrow classes
        this.bubble.classList.remove('arrow-right', 'arrow-bottom');

        // Check right-edge overflow (viewport-relative)
        if (rect.left + bubbleWidth > window.innerWidth - padding) {
            left = window.innerWidth - bubbleWidth - padding + window.scrollX;
            this.bubble.classList.add('arrow-right');
        }

        // Check bottom overflow — show above if no room below (viewport-relative)
        if (rect.bottom + padding + bubbleHeight > window.innerHeight - padding) {
            top = rect.top - bubbleHeight - padding + window.scrollY;
            this.bubble.classList.add('arrow-bottom');
        }

        this.bubble.style.left = `${Math.max(padding, left)}px`;
        this.bubble.style.top = `${Math.max(padding, top)}px`;

        // Point arrow at the icon center
        const iconCenterX = rect.left + rect.width / 2 + window.scrollX;
        const bubbleLeft = Math.max(padding, left);
        const arrowOffset = Math.max(10, Math.min(iconCenterX - bubbleLeft - 6, bubbleWidth - 22));
        this.bubble.style.setProperty('--arrow-left', `${arrowOffset}px`);
    },

    show(icon, fromClick = false) {
        const tooltipId = icon.getAttribute('data-tooltip');
        const content = this.tooltips[tooltipId];

        if (!content) return;

        // If already showing this tooltip, don't re-trigger
        if (this.activeIcon === icon && this.bubble.classList.contains('visible')) {
            return;
        }

        // Hide any existing tooltip first
        if (this.activeIcon && this.activeIcon !== icon) {
            this.activeIcon.classList.remove('active');
        }

        // Update content
        this.bubble.innerHTML = `
            <div class="tooltip-title">${content.title}</div>
            <div class="tooltip-text">${content.text}</div>
        `;

        // Temporarily make bubble visible off-screen for accurate height measurement
        this.bubble.style.visibility = 'hidden';
        this.bubble.style.display = 'block';
        this.bubble.style.opacity = '0';
        this.bubble.classList.add('visible');

        this._position(icon);

        // Restore visibility
        this.bubble.style.visibility = '';
        this.bubble.style.display = '';
        this.bubble.style.opacity = '';

        // Only show overlay on click (not hover) to prevent flickering
        if (fromClick) {
            this.overlay.classList.remove('hidden');
            this.triggeredByClick = true;
        } else {
            this.overlay.classList.add('hidden');
            this.triggeredByClick = false;
        }

        this.bubble.classList.add('visible');
        icon.classList.add('active');
        this.activeIcon = icon;
    },

    hide() {
        // Don't hide if triggered by click and still hovering
        if (this.triggeredByClick && this.isHovering) {
            return;
        }
        this._doHide();
    },

    forceHide() {
        // Force close regardless of hover state (used for click-outside and Escape)
        this._doHide();
    },

    _doHide() {
        this.overlay.classList.add('hidden');
        this.bubble.classList.remove('visible');
        if (this.activeIcon) {
            this.activeIcon.classList.remove('active');
            this.activeIcon = null;
        }
        this.triggeredByClick = false;
        this.isHovering = false;
    }
};

// Initialize tooltip system after DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    TooltipManager.init();
});

// ============================================
// Onboarding Banner
// ============================================
function initOnboarding() {
    const banner = document.getElementById('onboardingBanner');
    const dismissBtn = document.getElementById('dismissOnboarding');
    const helpLink = document.getElementById('helpLink');

    if (!banner) return;

    // Check if user has dismissed before
    const dismissed = localStorage.getItem('varaOnboardingDismissed');
    if (dismissed) {
        banner.classList.add('hidden');
    }

    // Dismiss button handler
    if (dismissBtn) {
        dismissBtn.addEventListener('click', () => {
            banner.classList.add('hidden');
            localStorage.setItem('varaOnboardingDismissed', 'true');
        });
    }

    // Help link handled by shared hld-modal.js
}

document.addEventListener('DOMContentLoaded', initOnboarding);

// HLD Modal is now handled by the shared hld-modal.js

/**
 * Shared Navbar Component
 * Dynamically injects a consistent header and footer across all pages.
 */
(function () {
    // Detect current page to set active nav link and correct paths
    const path = window.location.pathname;
    const isPolicy = path.includes('policy-knowledge-base');
    const basePath = isPolicy ? '../' : '';
    const policyPath = isPolicy ? '#' : 'policy-knowledge-base';
    const emailPath = isPolicy ? '../' : '#';

    // Subtitle per page
    const subtitle = isPolicy ? 'Policy Compiler Agent' : 'AI based Customer Support';

    // Favicon path (both folders have a copy)
    const faviconSrc = 'favicon.png';

    const headerHTML = `
    <header class="header">
        <div class="header-content">
            <div class="logo">
                <div class="logo-icon" style="overflow: hidden;">
                    <img src="${faviconSrc}" alt="Logo" style="width: 100%; height: 100%; object-fit: contain;">
                </div>
                <div class="logo-text">
                    <span class="logo-title">VARA-AI</span>
                    <span class="logo-subtitle">${subtitle}</span>
                </div>
            </div>
            <nav class="header-nav">
                <a href="${emailPath}" class="nav-link${!isPolicy ? ' active' : ''}">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
                    </svg>
                    <span>Email Pipeline</span>
                </a>
                <a href="${policyPath}" class="nav-link${isPolicy ? ' active' : ''}">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <circle cx="18" cy="5" r="3" />
                        <circle cx="6" cy="12" r="3" />
                        <circle cx="18" cy="19" r="3" />
                        <line x1="8.59" y1="13.51" x2="15.42" y2="17.49" />
                        <line x1="15.41" y1="6.51" x2="8.59" y2="10.49" />
                    </svg>
                    <span>Policy Knowledge Base</span>
                </a>
            </nav>
            <a href="#" class="help-link" id="helpLink">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"
                    stroke-linejoin="round">
                    <circle cx="12" cy="12" r="10" />
                    <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
                    <line x1="12" y1="17" x2="12.01" y2="17" />
                </svg>
                How it works
            </a>
        </div>
    </header>`;

    const footerHTML = `
    <footer class="site-footer">
        <div class="footer-content">
            <span class="copyright">&copy; 2026 VARA-AI. </span>
            <span class="footer-divider">|</span>
            <span class="footer-tagline">Powered by Amazon Nova &amp; Neo4j</span>
        </div>
    </footer>`;

    // Insert header at the navbar placeholder (or as first child of body)
    const navPlaceholder = document.getElementById('navbar-placeholder');
    if (navPlaceholder) {
        navPlaceholder.outerHTML = headerHTML;
    } else {
        // Fallback: insert after the background effects (orbs etc.)
        const body = document.body;
        const firstNonBg = body.querySelector('.main-container, .onboarding-banner, section');
        if (firstNonBg) {
            firstNonBg.insertAdjacentHTML('beforebegin', headerHTML);
        } else {
            body.insertAdjacentHTML('afterbegin', headerHTML);
        }
    }

    // Insert footer at the footer placeholder (or as last child of body)
    const footerPlaceholder = document.getElementById('footer-placeholder');
    if (footerPlaceholder) {
        footerPlaceholder.outerHTML = footerHTML;
    } else {
        // Fallback: append before closing </body>
        const existingFooter = document.querySelector('.site-footer');
        if (!existingFooter) {
            document.body.insertAdjacentHTML('beforeend', footerHTML);
        }
    }

    // ── Hide navbar on scroll-down, show on scroll-up / at top ──
    const header = document.querySelector('.header');
    if (header) {
        let ticking = false;

        window.addEventListener('scroll', () => {
            if (!ticking) {
                requestAnimationFrame(() => {
                    const currentScrollY = window.scrollY;
                    if (currentScrollY <= 0) {
                        // At the very top — show navbar
                        header.classList.remove('header-hidden');
                    } else {
                        // Any scroll away from top — hide navbar
                        header.classList.add('header-hidden');
                    }
                    ticking = false;
                });
                ticking = true;
            }
        }, { passive: true });
    }
})();

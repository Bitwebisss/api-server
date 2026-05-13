(function() {
    'use strict';
    // Private constants
    const THEME_COOKIE_NAME = 'theme';
    const COOKIE_DAYS = 365;
    // Helper: set cookie
    function setCookie(name, value, days) {
        const expires = days ? `; expires=${new Date(Date.now() + days * 864e5).toGMTString()}` : '';
        document.cookie = `${encodeURIComponent(name)}=${encodeURIComponent(value)}${expires}; path=/`;
    }
    // Helper: get cookie
    function getCookie(name) {
        const prefix = `${encodeURIComponent(name)}=`;
        const cookies = document.cookie.split(';');
        for (const cookie of cookies) {
            let c = cookie.trim();
            if (c.startsWith(prefix)) {
                return decodeURIComponent(c.substring(prefix.length));
            }
        }
        return null;
    }
    // Apply theme to DOM
    function applyTheme(mode) {
        let resolved = mode;
        if (mode === 'auto') {
            resolved = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
        }
        document.documentElement.setAttribute('data-bs-theme', resolved);
        const labels = { light: '☀️ LIGHT', dark: '🌙 DARK', auto: '🔄 AUTO' };
        const themeLabel = document.getElementById('theme-label');
        if (themeLabel) themeLabel.textContent = labels[mode] || '🔄 AUTO';
    }
    // Public API exposed via global (minimal)
    window.themeManager = {
        setTheme: function(mode) {
            setCookie(THEME_COOKIE_NAME, mode, COOKIE_DAYS);
            applyTheme(mode);
        },
        init: function() {
            const savedTheme = getCookie(THEME_COOKIE_NAME) || 'auto';
            applyTheme(savedTheme);
            // Listen for system theme changes when in 'auto' mode
            window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
                const current = getCookie(THEME_COOKIE_NAME) || 'auto';
                if (current === 'auto') applyTheme('auto');
            });
            // Attach event listeners to theme dropdown items
            const themeItems = document.querySelectorAll('[data-theme]');
            for (const item of themeItems) {
                item.addEventListener('click', (e) => {
                    e.preventDefault();
                    const mode = item.getAttribute('data-theme');
                    if (mode) window.themeManager.setTheme(mode);
                });
            }
        }
    };
    // Auto-initialize after DOM ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => window.themeManager.init());
    } else {
        window.themeManager.init();
    }
})();

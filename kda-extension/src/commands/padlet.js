const { createWebviewPanel } = require('../utils/webview');

class PadletCommand {
    constructor() {
        this.disposables = [];
    }

    execute() {
        const url = 'https://padlet.com/dlwjdtn410/padlet-p76oleydm4y0573v'; // 예시 URL
        createWebviewPanel(url);
    }

    dispose() {
        this.disposables.forEach(d => d.dispose());
    }
}

module.exports = PadletCommand;

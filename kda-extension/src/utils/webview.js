const vscode = require('vscode');

function createWebviewPanel(url) {
    const panel = vscode.window.createWebviewPanel(
        'padletWebview',
        'Padlet View',
        vscode.ViewColumn.Two,
        {
            enableScripts: true,
            // 웹뷰가 숨겨져 있을 때도 컨텍스트를 유지합니다
            retainContextWhenHidden: true,
            // 웹뷰에서 접근할 수 있는 로컬 리소스의 루트 경로를 지정합니다
            localResourceRoots: [vscode.Uri.file(__dirname)]
        }
    );

    panel.webview.html = _getWebviewContent(url);
}

function _getWebviewContent(url) {
    return `<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="Content-Security-Policy" content="default-src * 'unsafe-inline' 'unsafe-eval' data: blob:;">
        <title>Padlet View</title>
        <style>
            body, html {
                margin: 0;
                padding: 0;
                width: 100%;
                height: 100%;
                overflow: hidden;
            }
            iframe {
                width: 100%;
                height: 100%;
                border: none;
                position: absolute;
                top: 0;
                left: 0;
            }
        </style>
    </head>
    <body>
        <iframe src="${url}" sandbox="allow-same-origin allow-scripts allow-popups allow-forms allow-downloads"></iframe>
    </body>
    </html>`;
}

module.exports = {
    createWebviewPanel
};

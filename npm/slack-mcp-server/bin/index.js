#!/usr/bin/env node

const childProcess = require('child_process');

const BINARY_MAP = {
    darwin_x64: {name: 'slack-mcp-server-darwin-amd64', suffix: ''},
    darwin_arm64: {name: 'slack-mcp-server-darwin-arm64', suffix: ''},
    linux_x64: {name: 'slack-mcp-server-linux-amd64', suffix: ''},
    linux_arm64: {name: 'slack-mcp-server-linux-arm64', suffix: ''},
    win32_x64: {name: 'slack-mcp-server-windows-amd64', suffix: '.exe'},
    win32_arm64: {name: 'slack-mcp-server-windows-arm64', suffix: '.exe'},
};

// Resolving will fail if the optionalDependency was not installed or the platform/arch is not supported
const resolveBinaryPath = () => {
    // If DXT installation then we fix empty variables, it's DXT bug.
    if (process.env.SLACK_MCP_DXT) {
        if (process.env.SLACK_MCP_XOXC_TOKEN === '${user_config.xoxc_token}') {
            process.env.SLACK_MCP_XOXC_TOKEN = '';
        }
        if (process.env.SLACK_MCP_XOXD_TOKEN === '${user_config.xoxd_token}') {
            process.env.SLACK_MCP_XOXD_TOKEN = '';
        }
        if (process.env.SLACK_MCP_XOXP_TOKEN === '${user_config.xoxp_token}') {
            process.env.SLACK_MCP_XOXP_TOKEN = '';
        }
        if (process.env.SLACK_MCP_ADD_MESSAGE_TOOL === '${user_config.add_message_tool}') {
            process.env.SLACK_MCP_ADD_MESSAGE_TOOL = '';
        }
    }

    try {
        const binary = BINARY_MAP[`${process.platform}_${process.arch}`];

        if (process.env.SLACK_MCP_DXT) {
            return require.resolve(__dirname + `/${binary.name}${binary.suffix}`);
        } else {
            return require.resolve(`${binary.name}/bin/${binary.name}${binary.suffix}`);
        }
    } catch (e) {
        throw new Error(`Could not resolve binary path for platform/arch: ${process.platform}/${process.arch}`);
    }
};

childProcess.execFileSync(resolveBinaryPath(), process.argv.slice(2), {
    stdio: 'inherit',
});

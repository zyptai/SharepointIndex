import globals from "globals";
import pluginJs from "@eslint/js";

export default [
  {
    languageOptions: {
      globals: globals.node, // Add Node.js global objects
    },
    plugins: {
      js: pluginJs.configs.recommended,
    },
    env: {
      node: true,  // Set the environment to Node.js
    },
  },
];

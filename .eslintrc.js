module.exports = {
  "globals": {
    __PATH_PREFIX__: true,
  },
  "env": {
    "browser": true,
    "es2021": true
  },
  "extends": [
    "react-app",  // Only for gatsby apps
    "eslint:recommended",
  ],
  "parserOptions": {
    "ecmaVersion": "latest",
    "sourceType": "module"
  },
}

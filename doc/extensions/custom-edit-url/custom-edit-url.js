// Construct a URL for the 'Edit this page' link in the header of each page.
// Use this to direct contributions to a different public repo from the one that contains the asciidoc source.
// In a playbook, include the extension as follows:
// antora:
//   extensions:
//   - require: "./extensions/custom-edit-url"
//     repo: <REPO_URL>
//     start_path: <START_PATH>
//     link_to_edit_page: <true|false>
//
// where:
//   REPO_URL is the full URL of the public repo
//   [OPTIONAL] START_PATH is the path to the folder containing the asciidoc source files in the public repo.
//   [OPTIONAL] link_to_edit_page (boolean, default = false) when true, the link points to the edit page on github.


module.exports.register = function ({ config }) {
  const logger = this.getLogger('custom-edit-url')
  this.on('documentsConverted', ({ contentCatalog }) => {
    const { playbook } = this.getVariables()
    const { repo = playbook.content.sources[0].url, startPath = '', linkToEditPage = false } = config
    const editOrTree = linkToEditPage ? 'edit' : 'tree'
    contentCatalog.getPages((page) => page.out).forEach((page) => {
      const urlParts = [
        repo,
        editOrTree,
        page.src.origin.refname,
        startPath,
        page.src.path
      ].filter(Boolean)
      page.src.editUrl = urlParts.join('/')     
    })
  })
}

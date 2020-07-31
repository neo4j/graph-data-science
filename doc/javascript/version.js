window.versions = (async function() {
  console.log("Calling versions()")
  const versionsUrl =
    'https://s3-eu-west-1.amazonaws.com/com.neo4j.graphalgorithms.dist/graph-data-science/doc-versions.json';
  const versions = await jQuery.ajax({
    type: 'GET',
    url: versionsUrl,
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/vnd.github.v3+json'
    },
    crossOrigin: true,
    success: function (data) {
      return data;
    },
    error: function (jqXHR, textStatus, errorThrown) {
      console.error('Request failed...', jqXHR, textStatus, errorThrown);
    }
  });

  return {
    value: versions
  };
})();

window.docMeta = (async function () {
  console.log("1 window.docMeta")
  const version = '1.4-preview';
  const name = 'graph-data-science';
  const href = window.location.href;

  return {
    name: name,
    version: version,
    availableDocVersions: window.versions,
    thisPubBaseUri: href.substring(0, href.indexOf(name) + name.length) + '/' + version,
    unversionedDocBaseUri: href.substring(0, href.indexOf(name) + name.length) + '/',
    commonDocsBaseUri: href.substring(0, href.indexOf(name) - 1)
  }

})();

(function () {
  const pathname = window.location.pathname;
  if (pathname.indexOf(window.docMeta.name) > -1) {
    const baseUri = window.docMeta.unversionedDocBaseUri + pathname.split(window.docMeta.name + '/')[1].split('/')[0] + '/';
    window.neo4jPageId = window.location.href.replace(baseUri, '');
  }
})();
// vim: set sw=2 ts=2:

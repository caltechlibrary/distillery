{##}
{# this is a Jinja template #}
{##}
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@next/css/pico.min.css">
    <title>{{ title }}</title>
    <script src="https://cdn.jsdelivr.net/npm/universalviewer@4.0.17/dist/umd/UV.min.js"></script>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/universalviewer@4.0.17/dist/uv.min.css">
    <style>/* reset progressive block spacing */
      @media (min-width: 1536px) {
        body > footer, body > header, body > main, section {
          --pico-block-spacing-vertical: calc(var(--pico-spacing) * 2);
        }
      }
      @media (min-width: 1280px) {
        body > footer, body > header, body > main, section {
          --pico-block-spacing-vertical: calc(var(--pico-spacing) * 2);
        }
      }
      @media (min-width: 1024px) {
        body > footer, body > header, body > main, section {
          --pico-block-spacing-vertical: calc(var(--pico-spacing) * 2);
        }
      }
      @media (min-width: 768px) {
        body > footer, body > header, body > main, section {
          --pico-block-spacing-vertical: calc(var(--pico-spacing) * 2);
        }
      }
      @media (min-width: 576px) {
        body > footer, body > header, body > main, section {
          --pico-block-spacing-vertical: calc(var(--pico-spacing) * 2);
        }
      }
    </style>
    <style>/* override pico styles for uv elements */
      #uv button {
        width: initial;
      }
      #uv [role="button"] {
        background-color: initial;
        border: initial;
        border-radius: initial;
        cursor: pointer;
        opacity: initial;
        padding: initial;
        pointer-events: initial;
      }
      #uv h2 {
        color: inherit;
      }
      #uv input {
        border-radius: initial;
      }
      #uv .headerPanel .mode label {
        width: auto;
      }
      #uv .headerPanel input {
        background-color: revert;
        border: initial;
        text-align: center;
      }
      #uv .headerPanel .search {
        width: auto;
      }
      #uv .headerPanel .search .btn {
        padding-block-start: 4px;
        padding-block-end: 0;
      }
      #uv label {
        overflow-wrap: initial;
      }
      #uv .overlays {
        display: flex;
        align-items: center;
        justify-content: center;
      }
      #uv .overlay .btn {
        padding: 10px 15px;
      }
      #uv .overlay.download {
        width: 300px;
      }
      #uv .overlay.settings {
        height: auto;
        position: unset;
      }
      #uv .overlay.settings .scroll {
        height: auto;
        margin-block-end: var(--pico-form-element-spacing-vertical);
      }
      #uv .overlay.settings .setting select {
        height: auto;
        padding: var(--pico-form-element-spacing-vertical) var(--pico-form-element-spacing-horizontal);
        padding-inline-end: calc(var(--pico-form-element-spacing-horizontal) + 1.5rem);
      }
      #uv .overlay.settings .setting input[type="checkbox"] {
        margin-inline-start: var(--pico-outline-width);
      }
      #uv .iiif-gallery-component input[type="range"]::-moz-range-track {
        width: auto;
      }
      /* many elements need font-size adjustments */
      #uv .overlay .heading {
        font-size: 20px;
      }
      #uv .overlay h2 {
        font-size: 18px;
      }
      #uv .iiif-gallery-component .thumb .label,
      #uv .headerPanel .mode label,
      #uv .headerPanel input,
      #uv .headerPanel .total,
      #uv .headerPanel .search .btn,
      #uv .overlay.settings .setting,
      #uv .overlay.settings .setting select,
      #uv .overlay button {
        font-size: 16px;
      }
      #uv .overlay.settings .version,
      #uv .overlay.settings .website {
        font-size: 14px;
      }
    </style>
    <style>/* header with logo and menu */
      body > header > nav {
        flex-wrap: wrap;
      }
      @media (min-width: 992px) {
        body > header > nav {
          flex-wrap: nowrap;
        }
        body > header > nav > ul:first-of-type > li {
          width: 480px;
        }
      }
      body > header > nav > ul {
        align-items: flex-end;
      }
      body > header > nav > ul:first-of-type > li > svg {
        overflow: visible;
      }
      body > header > nav > ul:first-of-type > li > svg > a > rect {
        fill: transparent;
        stroke: transparent;
        transition: fill var(--pico-transition), stroke var(--pico-transition);
      }
      body > header > nav > ul:first-of-type > li > svg > a:focus-visible > rect {
        stroke: var(--pico-primary-focus);
        stroke-width: calc(var(--pico-outline-width) * 1.5);
      }
      body > header > nav > ul:first-of-type > li > svg > a:hover > rect {
        fill: var(--pico-primary-focus);
      }
      body > header > nav > ul:first-of-type > li > svg > a:first-of-type > path {
        fill: #ff6c0c;
        transition: fill var(--pico-transition);
      }
      body > header > nav > ul:first-of-type > li > svg > a:first-of-type:hover > path {
        fill: var(--pico-h1-color);
      }
      body > header > nav > ul:first-of-type > li > svg > a:last-of-type > path {
        fill: var(--pico-h1-color);
        stroke: var(--pico-h1-color);
      }
      body > header > nav > ul:last-child {
        margin-inline-start: auto;
      }
      body > header > nav > ul:last-child > li :where(a, [role="link"]) {
        border-radius: 0;
      }
    </style>
    <style>/* content */
      :root {
        --pico-nav-breadcrumb-divider: "/";
      }
      hgroup h1 {
        margin-block-end: var(--pico-typography-spacing-vertical);
      }
      hgroup p:last-child span {
        padding: var(--pico-nav-element-spacing-vertical) var(--pico-nav-element-spacing-horizontal);
      }
      hgroup p:last-child span:first-child {
        padding-inline-start: 0;
      }
      hgroup p:last-child span:not(:first-child) {
        -webkit-margin-start: var(--pico-nav-link-spacing-horizontal);
        margin-inline-start: var(--pico-nav-link-spacing-horizontal);
      }
      hgroup p:last-child span:not(:last-child)::after {
        position: absolute;
        width: calc(var(--pico-nav-link-spacing-horizontal) * 2);
        -webkit-margin-start: calc(var(--pico-nav-link-spacing-horizontal) / 2);
        margin-inline-start: calc(var(--pico-nav-link-spacing-horizontal) / 2);
        content: var(--pico-nav-breadcrumb-divider);
        color: var(--pico-muted-color);
        text-align: center;
        text-decoration: none;
      }
      #manifest {
        border: var(--pico-outline-width) solid transparent;
        display: inline-block;
      }
      #manifest:hover {
        background-color: rgba(40, 115, 171, 0.25); /* translucent blue in IIIF logo */
      }
    </style>
    <style>/* colors */
      /* Orange color for light color scheme (Default) */
      /* Can be forced with data-theme="light" */
      [data-theme=light],
      :root:not([data-theme=dark]) {
        --pico-text-selection-color: rgba(244, 93, 44, 0.25);
        --pico-primary: #bd3c13;
        --pico-primary-background: #d24317;
        --pico-primary-underline: rgba(189, 60, 19, 0.5);
        --pico-primary-hover: #942d0d;
        --pico-primary-hover-background: #bd3c13;
        --pico-primary-focus: rgba(244, 93, 44, 0.25);
        --pico-primary-inverse: #fff;
      }
      /* Orange color for dark color scheme (Auto) */
      /* Automatically enabled if user has Dark mode enabled */
      @media only screen and (prefers-color-scheme: dark) {
        :root:not([data-theme]) {
          --pico-text-selection-color: rgba(245, 107, 61, 0.1875);
          --pico-primary: #f56b3d;
          --pico-primary-background: #d24317;
          --pico-primary-underline: rgba(245, 107, 61, 0.5);
          --pico-primary-hover: #f8a283;
          --pico-primary-hover-background: #e74b1a;
          --pico-primary-focus: rgba(245, 107, 61, 0.25);
          --pico-primary-inverse: #fff;
        }
      }
      /* Orange color for dark color scheme (Forced) */
      /* Enabled if forced with data-theme="dark" */
      [data-theme=dark] {
        --pico-text-selection-color: rgba(245, 107, 61, 0.1875);
        --pico-primary: #f56b3d;
        --pico-primary-background: #d24317;
        --pico-primary-underline: rgba(245, 107, 61, 0.5);
        --pico-primary-hover: #f8a283;
        --pico-primary-hover-background: #e74b1a;
        --pico-primary-focus: rgba(245, 107, 61, 0.25);
        --pico-primary-inverse: #fff;
      }
    </style>
  </head>
  <body>
    <header class="container">
      <nav>
        <ul>
          <li>
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 700.98 71.76" width="100%" fill="transparent">
              <a href="https://www.caltech.edu/" aria-label="Caltech Home" role="link">
                <rect x="-1%" y="-10%" width="45%" height="120%"></rect>
                <path data-text="Caltech" fill="#000" d="M300.86,82.27h11.58V57.6c0-5.88,2.65-13.28,11.38-13.28,5.88,0,8.54,2.85,8.54,9.49V82.27h11.57V50.49c0-14-9.39-17.36-17.55-17.36-6.54,0-11.19,2.56-13.76,5.88h-.18V12.82H300.86Zm-3.6-35.48c-2.95-7.21-8.64-13.66-20.31-13.66-15.56,0-24.57,10.81-24.57,25.14s9,25.14,23.72,25.14c13,0,18.5-7.68,21.72-14.71L288,63.49c-1.81,4.36-4.75,9.3-11.67,9.3-8.07,0-11.77-7-11.77-14.52S268.7,43.75,277,43.75a10.66,10.66,0,0,1,10.15,7.69ZM213,52.1a12.37,12.37,0,0,1,12.43-9.49c7.78,0,10.91,6.93,10.91,9.49ZM248.52,61c0-17.17-7-27.89-23.06-27.89-15.56,0-24.57,10.81-24.57,25.14s9,25.14,24.57,25.14c13.1,0,20-6.26,23.63-11.76L239.32,66c-1.14,1.62-4.75,6.84-13.86,6.84A12.2,12.2,0,0,1,213,61ZM178.37,23.81V71.27c0,7.3,3.23,11.57,12.72,11.57a42.7,42.7,0,0,0,9.2-1.14V72.22a49.78,49.78,0,0,1-5.12.57c-4.46,0-5.22-2.09-5.22-5.89V42.61h10.34V34.26H190v-22ZM159,82.27h11.57V12.82H159ZM139.12,65.1c0,6.17-6.93,8.82-12.34,8.82-4.17,0-7.49-2.37-7.49-6,0-4.56,3.6-5.79,8-6.64L133.61,60a20.89,20.89,0,0,0,5.51-1.71Zm11.57-16.22c0-13.67-10.34-15.75-20.68-15.75-10.53,0-20.5,4.36-20.5,15.65l11.58.48C121.09,44,123.27,42,130,42c5,0,9.11,1.33,9.11,6.08v1.42c-2.94,1-8.35,2-12.53,2.85l-4.84,1c-7.11,1.42-14.61,5.59-14.61,15s7.21,15.08,16.32,15.08a24.27,24.27,0,0,0,15.66-5.5,9.07,9.07,0,0,0,1.14,4.36h12.23c-.66-1-1.8-3.13-1.8-8.06ZM105.4,34.26C102.17,20.13,91.83,13,78.26,13c-20.21,0-32,16.42-32,35.49S56.63,84,78.26,84c13.48,0,22.49-6.55,27.9-20.11L94.58,58.55c-2.47,7.88-7.49,13.67-16.32,13.67-13.47,0-19.07-12-19.07-23.72s5.6-23.72,19.07-23.72c7.69,0,13.85,5.12,14.9,12.61Z" transform="translate(-46.23 -12.22)"></path>
              </a>
              <a href="https://archives.caltech.edu/" aria-label="Caltech Archives & Special Collections" role="link">
                <rect x="46%" y="-10%" width="55%" height="120%"></rect>
                <path data-text="A" fill="#000" stroke="#000" d="M458.81,30c-5.26-8.22-15.35-11.61-24.59-11.61C412,18.39,379,43.65,379,67.47c0,7.46,7.8,10.34,14.16,10.34,6.7,0,22-2.88,26.45-8.14.93-5.67,9.58-22.8,16.19-22.8,1.1,0,1.1,1.61,1.1,2.29,0,5-9.07,15.68-12.63,19.16l-.08.85c-.6,6.18,6.78,8.3,11.95,8.3,7.63,0,15-2.62,22.46-4.15V75c-8,1.69-15.93,4.49-24.16,4.49-6.1,0-12.2-2.29-14.66-8.31-6.87,5.17-16.87,8.22-25.43,8.22-12,0-19.16-5.85-19.16-18.31,0-9.41,7.46-19.49,14.16-25.6,12-11,29.33-18,45.69-18,8.81,0,19.32,2.54,24.66,10.17ZM435.33,49.25c-3.23-.09-8.48,11.69-9.24,14.58,2.79-1.7,9.91-10.68,9.91-13.91Z" transform="translate(-46.23 -12.22)"></path>
                <path data-text="r" fill="#000" stroke="#000" d="M483,77.05c-2.54,0-5.59-.59-5.59-3.9a5.87,5.87,0,0,1,.76-3.39,8.84,8.84,0,0,1-1.44.17,14.34,14.34,0,0,1-4.24-.85,43.28,43.28,0,0,1-14.66,5.85V73.32a45.34,45.34,0,0,0,13.56-5.26v-.42c0-1.44,0-3.22,1.86-3.22a1.21,1.21,0,0,1,1.19,1.19,3.94,3.94,0,0,1-.51,1.78c.77.84,2.21.93,3.31.93,2.2,0,4.49-1.36,6.53-1.27l.59.51c-1.36,1.44-3.48,3.73-3.48,5.84,0,1.79,2.46,2,3.73,2,3.22,0,7.38-1.27,10.51-2.12v1.61C491.11,76,487.13,77.05,483,77.05Z" transform="translate(-46.23 -12.22)"></path>
                <path data-text="c" fill="#000" stroke="#000" d="M515.27,77.81c-4.24,0-8.31-1.35-10.43-5.25l-10.59,2.37V73.32l10.25-2.46c1.36-4.91,5.94-7.63,10.86-7.63,2.79,0,5.59,1.61,5.59,4.66a4.25,4.25,0,0,1-4.58,4.5,2.91,2.91,0,0,1-2.29-1.61,49,49,0,0,0-5.08.93c0,2.71,5.42,4.07,7.54,4.07,4.83,0,9.58-1.53,14.24-2.46v1.61C525.7,76.12,520.61,77.81,515.27,77.81Zm-.08-12.54c-2.63,0-6.11,1.69-6.53,4.57l5.09-.84c.59-1.44,1.52-2.46,3-2.46a6,6,0,0,1,1.35.51,5.56,5.56,0,0,1,.43,1.1c0,.76-.51,1.19-.94,1.69l.26.09a1.72,1.72,0,0,0,1.52-1.87C519.42,66,516.88,65.27,515.19,65.27Z" transform="translate(-46.23 -12.22)"></path>
                <path data-text="h" fill="#000" stroke="#000" d="M559,62.3c-1.86,1.53-8.3,10.68-8.39,12.8l.26.09c7.29-2.29,20.77-7.8,27.8-7.8,1.27,0,3.31.34,3.31,2,0,2.46-2.12,4.07-2.38,6.1l.26.09,14.24-2.29v1.61c-5.43,1.1-10.77,2.88-16.36,2.88a6.46,6.46,0,0,1-2.8-.51,2.29,2.29,0,0,1-.25-.67c0-1.61,1.69-5,2.46-6.45a.8.8,0,0,0-.85-.84c-8.56,0-23.65,9.41-30.86,9.24l-.76-.43V77.9L550,67.73a175.57,175.57,0,0,1-20.26,7.2V73.32a120.38,120.38,0,0,0,19.07-7.12c4.16-2,4.07-2.88,6.61-6.53l19.84-28.73c2.54-3.65,12-14.67,16.36-14.67,2,0,2.8,2.29,2.8,4C594.45,33.9,569.1,53.82,559,62.3Zm31.88-43.66c-7.12,0-29.76,39.25-30.43,40.35,8.56-6,32.21-27.29,32.21-37.8C592.67,20.08,592.24,18.64,590.89,18.64Z" transform="translate(-46.23 -12.22)"></path>
                <path data-text="i" fill="#000" stroke="#000" d="M624.88,74.93c-5.25,1-10.51,2.71-15.76,2.71-1.36,0-3.22,0-3.22-1.78,0-1.1.59-2.12.59-3.22l-.42-.25h-.43c-4.32,0-8.73,1.78-12.88,2.54V73.32c4.32-.93,16.19-4.32,20-4.24l.6.34.25.42c-.85,1.61-5.34,3.9-5.34,5.35,0,.59,1.53.59,1.86.59,5.09,0,10.18-1.7,15.18-2.46Zm-4.74-29.5H620v-.34c0-1.78.76-6.7,3.22-6.7a1.73,1.73,0,0,1,1.69,1.53C624.88,41.79,622.09,45.43,620.14,45.43Z" transform="translate(-46.23 -12.22)"></path>
                <path data-text="v" fill="#000" stroke="#000" d="M657.86,77.9c-2.2,0-3.22-.34-4.66-2.12-3.56,2-8.48,4.15-12.63,4.15-2.12,0-4.41-1.1-4.41-3.47a8,8,0,0,1,1.19-3.9l-.26-.17h-.25c-4.58,0-9.41,1.78-13.9,2.54V73.4l20.43-3.89.51.33c0,1.7-3.06,3.82-3.06,6.36a2.8,2.8,0,0,0,2.46,1.19c7.72,0,10.09-8,12.89-8l.59.09a2.86,2.86,0,0,1,.68,1.1c0,1.69-1.61,2.79-1.78,4.07.42,1.27,1.86,1.35,3.05,1.35,5.09,0,10.17-1.78,15.26-2.71v1.61C668.63,76,663.37,77.9,657.86,77.9Z" transform="translate(-46.23 -12.22)"></path>
                <path data-text="e" fill="#000" stroke="#000" d="M691.69,77.81c-3.73,0-8-.76-10.26-4.15L673,74.93V73.32l8-1.78c1.35-5.43,8.22-8.14,13.22-8.14,1.78,0,4.49.6,4.49,2.8,0,4.83-9.83,5.76-13.3,6.53.16,2.62,4.57,2.71,6.35,2.71a77.17,77.17,0,0,0,16.53-2.12v1.61C702.79,76.2,697.37,77.81,691.69,77.81Zm2-12.46c-2.63,0-8.73,2.29-8.73,5.43h.51c2,0,10-2.21,10-4.58C695.5,65.61,694.15,65.35,693.72,65.35Z" transform="translate(-46.23 -12.22)"></path>
                <path data-text="s" fill="#000" stroke="#000" d="M731,77a12.53,12.53,0,0,1-6.86,2.45c-2.38,0-6.62-.84-6.62-4a2.74,2.74,0,0,1,1.19-2.12A55.72,55.72,0,0,0,730.6,75.1c.68-.93,1.61-1.95,1.61-3.22,0-2.12-2.63-2.71-4.32-2.71-7.38,0-13.65,5.17-20.77,5.76V73.32c6.69-2.46,16.7-5.93,23.73-5.93,2.46,0,6.19.67,6.19,3.9a4.79,4.79,0,0,1-2.46,3.81,49.42,49.42,0,0,0,12.63-1.78v1.61C744.25,76.37,734.41,77.22,731,77Zm-9.49-1.53-.59.42c0,1.7,1.78,2.21,3.13,2.21a6.46,6.46,0,0,0,4.41-1.61C727.63,76,722.8,75.52,721.53,75.44Z" transform="translate(-46.23 -12.22)"></path>
              </a>
            </svg>
          </li>
        </ul>
        <ul>
          <li><a href="{{ archivesspace_public_url }}">Collection Guides</a></li>
        </ul>
      </nav>
    </header>
    <main class="container">
      <hgroup>
        <h1>
          {{ title }}
        </h1>
        {% if dates %}
        <p>
          {% for date in dates %}
          <span>{{ date }}</span>{{ "; " if not loop.last else "" }}
          {% endfor %}
        </p>
        {% endif %}
        {% if collection %}
        <p>
          <span>{{ collection }}</span>{% if series %}<span>{{ series }}</span>{% endif %}{% if subseries %}<span>{{ subseries }}</span>{% endif %}
        </p>
        {% endif %}
      </hgroup>
      <p><a href="{{ archivesspace_public_url | trim('/') }}{{ archival_object_uri }}">Open the <cite>{{ title }}</cite> record in its archival context</a></p>
      <div class="uv" id="uv" style="width:100%;height:80vh"></div>
      <script>
        const data = {{ iiif_manifest_json }}
        uv = UV.init("uv", data);
        // override config using an inline json object
        uv.on("configure", function ({ config, cb }) {
          cb({
            options: { rightPanelEnabled: false, termsOfUseEnabled: false },
            modules: {
              footerPanel: { options: { shareEnabled: false } },
              openSeadragonCenterPanel: { options: { requiredStatementEnabled: false } },
            },
          });
        });
      </script>
      <dl id="metadata">
        <dt>Title</dt>
        <dd>{{ title }}</dd>
        {% if collection %}
        <dt>Collection</dt>
        <dd>{{ collection }}</dd>
        {% endif %}
        {% if series %}
        <dt>Series</dt>
        <dd>{{ series}}</dd>
        {% endif %}
        {% if subseries %}
        <dt>Sub-Series</dt>
        <dd>{{ subseries }}</dd>
        {% endif %}
        {% if dates %}
        <dt>Dates</dt>
        {% for date in dates %}
        <dd>{{ date }}</dd>
        {% endfor %}
        {% endif %}
        {% if creators %}
        <dt>Creators</dt>
        {% for creator in creators %}
        <dd>{{ creator }}</dd>
        {% endfor %}
        {% endif %}
        {% if extents %}
        <dt>Extents</dt>
        {% for extent in extents %}
        <dd>{{ extent }}</dd>
        {% endfor %}
        {% endif %}
        {% if subjects %}
        <dt>Subjects</dt>
        {% for subject in subjects %}
        <dd>{{ subject }}</dd>
        {% endfor %}
        {% endif %}
        {% if notes %}
        {% for note_label, note_contents in notes.items() if note_contents %}
        <dt>{{ note_label }}</dt>
        {% for note_content in note_contents %}
        <dd>{{ note_content }}</dd>
        {% endfor %}
        {% endfor %}
        {% endif %}
      </dl>
      <div><a id="manifest" href="{{ iiif_manifest_url }}">
        <svg width="32" viewBox="0 0 493.36 441.33" xmlns="http://www.w3.org/2000/svg" role="img">
          <title>IIIF Manifest</title>
          <g transform="matrix(1.3333 0 0 -1.3333 0 441.33)"><g transform="scale(.1)"><path d="m65.242 2178.8 710-263.75-1.25-1900-708.75 261.25v1902.5" fill="#2873ab"/><path d="m804.14 2640.1c81.441-240.91-26.473-436.2-241.04-436.2-214.56 0-454.51 195.29-535.95 436.2-81.434 240.89 26.48 436.18 241.04 436.18 214.57 0 454.51-195.29 535.95-436.18" fill="#2873ab"/><path d="m1678.6 2178.8-710-263.75 1.25-1900 708.75 261.25v1902.5" fill="#ed1d33"/><path d="m935.08 2640.1c-81.437-240.91 26.477-436.2 241.04-436.2 214.56 0 454.51 195.29 535.96 436.2 81.43 240.89-26.48 436.18-241.04 436.18-214.57 0-454.52-195.29-535.96-436.18" fill="#ed1d33"/><path d="m1860.2 2178.8 710-263.75-1.25-1900-708.75 261.25v1902.5" fill="#2873ab"/><path d="m2603.7 2640.1c81.45-240.91-26.47-436.2-241.03-436.2-214.58 0-454.52 195.29-535.96 436.2-81.44 240.89 26.48 436.18 241.03 436.18 214.57 0 454.51-195.29 535.96-436.18" fill="#2873ab"/><path d="m3700.2 3310v-652.5s-230 90-257.5-142.5c-2.5-247.5 0-336.25 0-336.25l257.5 83.75v-572.5l-258.61-92.5v-1335l-706.39-262.5v2360s-15 850 965 950" fill="#ed1d33"/></g></g>
        </svg>
      </a></div>
    </main>
    <footer class="container">
      <article>{{ rights }}</article>
    </footer>
  </body>
</html>

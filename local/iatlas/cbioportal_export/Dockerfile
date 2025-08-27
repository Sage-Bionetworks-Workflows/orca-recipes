# uv + Python 3.10 preinstalled
FROM ghcr.io/astral-sh/uv:python3.10-bookworm
WORKDIR /root/cbioportal_export/

RUN uv venv /opt/venv
# Use the virtual environment automatically
ENV VIRTUAL_ENV=/opt/venv
# Place entry points in the environment at the front of the path
ENV PATH="/opt/venv/bin:$PATH"

# Install dep
COPY pyproject.toml uv.lock* ./

# Install exactly what's locked (fails if lock is out of date)
RUN uv sync --frozen --no-dev

# copy code 
COPY . .

WORKDIR /root/

# clone dep repos
RUN git clone https://github.com/rxu17/datahub-study-curation-tools.git -b upgrade-to-python3
RUN git clone https://github.com/cBioPortal/cbioportal.git -b v6.3.2

WORKDIR /root/cbioportal_export/
# reference/

Place the following curated datasets here (not checked into git — add to .gitignore):

  high_bnf_communities.biom       — BIOM table of communities with measured high BNF rates
  bnf_measurements.csv            — Paired measured BNF values (e.g. acetylene reduction assay)
  high_soc_communities.biom       — Reference for carbon sequestration application
  suppressive_communities.biom    — Reference for pathogen suppression application

These files are used by validate_pipeline.py and community_similarity.py.

Sources:
  - Drinkwater & Snapp (2007) empirical BNF datasets
  - NEON soil microbiome + BNF measurement paired studies
  - EMP curated high-function communities

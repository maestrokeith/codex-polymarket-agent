import pytest
from strix_engine.core import Candidate, Consensus, AgentResult, MomentumAgent, Settings

@pytest.mark.asyncio
async def test_momentum_score_is_bounded():
    c=Candidate(mint='x',liquidity_usd=50000,volume_5m=25000,buyers_5m=80,sellers_5m=20)
    r=await MomentumAgent().run(c)
    assert 0 <= r.score <= 1

def test_consensus_security_veto_rejects():
    cfg=Settings(min_confidence=.5); c=Candidate(mint='x')
    final=Consensus(cfg).decide(c,[AgentResult(agent='A',score=.95,veto=True),AgentResult(agent='B',score=.95),AgentResult(agent='C',score=.95)])
    assert final.veto is True

def test_consensus_allows_high_score_without_veto():
    cfg=Settings(min_confidence=.7); c=Candidate(mint='x')
    final=Consensus(cfg).decide(c,[AgentResult(agent='A',score=.95),AgentResult(agent='B',score=.85),AgentResult(agent='C',score=.80)])
    assert final.veto is False and final.score >= .7
